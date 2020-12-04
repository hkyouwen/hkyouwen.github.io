Task的数据传输包括Task内部传输和Task之间的传输。

Task内部传输是 Task 内部包含的多个不同的算子之间的数据传递，通过函数调用的参数来传递数据。

首先，要看一下 `Output` 接口，`Output` 接口继承自 `Collector` 接口，用于接受 Operator 提交的数据

```
public interface Output<T> extends Collector<T> {

   /**
    * Emits a {@link Watermark} from an operator. This watermark is broadcast to all downstream
    * operators.
    *
    * <p>A watermark specifies that no element with a timestamp lower or equal to the watermark
    * timestamp will be emitted in the future.
    */
   void emitWatermark(Watermark mark);

   /**
    * Emits a record the side output identified by the given {@link OutputTag}.
    *
    * @param record The record to collect.
    */
   <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record);

   void emitLatencyMarker(LatencyMarker latencyMarker);
}
```

在 `OperatorChain` 内部还有一个 `WatermarkGaugeExposingOutput` 接口继承自 `Output`，它主要是额外提供了一个获取 watermark 值的方法：

```
public interface WatermarkGaugeExposingOutput<T> extends Output<T> {
   Gauge<Long> getWatermarkGauge();
}
```

前面说过，在OperatorChain内部创建当前节点的下游节点时，会调用 createOutputCollector 为当前节点创建 output， ExecutionConfig有一个配置项，即 objectReuse，在默认情况下会禁止对象重用。如果不允许对象重用，则不会使用 ChainingOutput，而是会使用 CopyingChainingOutput。顾名思义，它和 ChainingOutput的区别在于，它会对记录进行拷贝后传递给下游算子

```
private <IN, OUT> WatermarkGaugeExposingOutput<StreamRecord<IN>> createChainedOperator(
      StreamTask<OUT, ?> containingTask,
      StreamConfig operatorConfig,
      Map<Integer, StreamConfig> chainedConfigs,
      ClassLoader userCodeClassloader,
      Map<StreamEdge, RecordWriterOutput<?>> streamOutputs,
      List<StreamOperator<?>> allOperators,
      OutputTag<IN> outputTag,
      MailboxExecutorFactory mailboxExecutorFactory) {
   // create the output that the operator writes to first. this may recursively create more operators
   WatermarkGaugeExposingOutput<StreamRecord<OUT>> chainedOperatorOutput = createOutputCollector(
      containingTask,
      operatorConfig,
      chainedConfigs,
      userCodeClassloader,
      streamOutputs,
      allOperators,
      mailboxExecutorFactory);

   // now create the operator and give it the output collector to write its output to
   OneInputStreamOperator<IN, OUT> chainedOperator = StreamOperatorFactoryUtil.createOperator(
         operatorConfig.getStreamOperatorFactory(userCodeClassloader),
         containingTask,
         operatorConfig,
         chainedOperatorOutput);

   allOperators.add(chainedOperator);

   WatermarkGaugeExposingOutput<StreamRecord<IN>> currentOperatorOutput;
   //判断objectReuse
   if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
      currentOperatorOutput = new ChainingOutput<>(chainedOperator, this, outputTag);
   }
   else {
      TypeSerializer<IN> inSerializer = operatorConfig.getTypeSerializerIn1(userCodeClassloader);
      currentOperatorOutput = new CopyingChainingOutput<>(chainedOperator, inSerializer, outputTag, this);
   }

   ......
   return currentOperatorOutput;
}
```

createOutputCollector将createChainedOperator返回的Output封装为一组outputs。`BroadcastingOutputCollector` 封装了一组 `Output`, 即 `Output<StreamRecord<T>>[] outputs`, 在接收到 `StreamRecord` 时，会将消息提交到所有的 内部所有的 `Output` 中。`BroadcastingOutputCollector`主要用在当前算子有多个下游算子的情况下。与此对应的还有一个 `CopyingBroadcastingOutputCollector`。

`DirectedOutput` 基于 `OutputSelector<OUT>[] outputSelectors` 选择要转发的目标 `Output`，主要是在 split/select 的情况下使用。与 `DirectedOutput` 对应的也有一个 `CopyingDirectedOutput`。

```
if (selectors == null || selectors.isEmpty()) {
   // simple path, no selector necessary
   if (allOutputs.size() == 1) {
      return allOutputs.get(0).f0;
   }
   else {
      // send to N outputs. Note that this includes the special case
      // of sending to zero outputs
      @SuppressWarnings({"unchecked", "rawtypes"})
      Output<StreamRecord<T>>[] asArray = new Output[allOutputs.size()];
      for (int i = 0; i < allOutputs.size(); i++) {
         asArray[i] = allOutputs.get(i).f0;
      }

      // This is the inverse of creating the normal ChainingOutput.
      // If the chaining output does not copy we need to copy in the broadcast output,
      // otherwise multi-chaining would not work correctly.
      if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
         return new CopyingBroadcastingOutputCollector<>(asArray, this);
      } else  {
         return new BroadcastingOutputCollector<>(asArray, this);
      }
   }
}
else {
   // selector present, more complex routing necessary

   // This is the inverse of creating the normal ChainingOutput.
   // If the chaining output does not copy we need to copy in the broadcast output,
   // otherwise multi-chaining would not work correctly.
   if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
      return new CopyingDirectedOutput<>(selectors, allOutputs);
   } else {
      return new DirectedOutput<>(selectors, allOutputs);
   }

}
```

从之前的文章知道StreamOperator的processElement是调用StreamOperator的output的collect方法

```
public class StreamMap<IN, OUT>
      extends AbstractUdfStreamOperator<OUT, MapFunction<IN, OUT>>
      implements OneInputStreamOperator<IN, OUT> {

   private static final long serialVersionUID = 1L;

   public StreamMap(MapFunction<IN, OUT> mapper) {
      super(mapper);
      chainingStrategy = ChainingStrategy.ALWAYS;
   }

   @Override
   public void processElement(StreamRecord<IN> element) throws Exception {
      output.collect(element.replace(userFunction.map(element.getValue())));
   }
}
```

所以OperatorChain内部数据传递是就是调用ChainingOutput的collect，当 `ChainingOutput` 接收到当前算子提交的数据时，直接将调用下游算子的 `processElement` 方法:

```
static class ChainingOutput<T> implements WatermarkGaugeExposingOutput<StreamRecord<T>> {

   ......

   @Override
   public void collect(StreamRecord<T> record) {
   //如果有 OutputTag， 则要求 OutputTag 匹配才会转发记录
      if (this.outputTag != null) {
         // we are not responsible for emitting to the main output.
         return;
      }

      pushToOperator(record);
   }

   @Override
   public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
      if (this.outputTag == null || !this.outputTag.equals(outputTag)) {
         // we are not responsible for emitting to the side-output specified by this
         // OutputTag.
         return;
      }

      pushToOperator(record);
   }

   protected <X> void pushToOperator(StreamRecord<X> record) {
      try {
         // we know that the given outputTag matches our OutputTag so the record
         // must be of the type that our operator expects.
         @SuppressWarnings("unchecked")
         StreamRecord<T> castRecord = (StreamRecord<T>) record;

//直接调用下游算子的 processElement 方法
         numRecordsIn.inc();
         operator.setKeyContextElement1(castRecord);
         operator.processElement(castRecord);
      }
      catch (Exception e) {
         throw new ExceptionInChainedOperatorException(e);
      }
   }


}
```

对于位于 `OperatorChain` 末尾的算子，它处理过的记录需要被其它 `Task` 消费，因此它的记录需要被写入 `ResultPartition` 。因此，Flink 提供了 `RecordWriterOutput`，它也实现了 `WatermarkGaugeExposingOutput`， 但是它是通过 `RecordWriter` 输出接收到的消息记录。`RecordWriter` 是 `ResultPartitionWriter` 的一层包装，提供了将记录序列化到 buffer 中的功能。

```
RecordWriterOutput<?> streamOutput = createStreamOutput(
   recordWriterDelegate.getRecordWriter(i),
   outEdge,
   chainedConfigs.get(outEdge.getSourceId()),
   containingTask.getEnvironment());
```

task之间的数据传递调用RecordWriterOutput的collect方法，最终调用RecordWriter的emit

```
public RecordWriterOutput(
      RecordWriter<SerializationDelegate<StreamRecord<OUT>>> recordWriter,
      TypeSerializer<OUT> outSerializer,
      OutputTag outputTag,
      StreamStatusProvider streamStatusProvider) {

   ......

@Override
public void collect(StreamRecord<OUT> record) {
   if (this.outputTag != null) {
      // we are not responsible for emitting to the main output.
      return;
   }

   pushToRecordWriter(record);
}

@Override
public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
   if (this.outputTag == null || !this.outputTag.equals(outputTag)) {
      // we are not responsible for emitting to the side-output specified by this
      // OutputTag.
      return;
   }

   pushToRecordWriter(record);
}

private <X> void pushToRecordWriter(StreamRecord<X> record) {
   serializationDelegate.setInstance(record);

   try {
      recordWriter.emit(serializationDelegate);
   }
   catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
   }
}
```

Task 通过 `RecordWriter` 将结果写入 `ResultPartition` 中。`RecordWriter` 是对 `ResultPartitionWriter`的一层封装，并负责将记录对象序列化到 buffer 中。先来看一下 `RecordWriter` 的成员变量和构造函数

```
public abstract class RecordWriter<T extends IOReadableWritable> implements AvailabilityProvider {

   /** Default name for the output flush thread, if no name with a task reference is given. */
   @VisibleForTesting
   public static final String DEFAULT_OUTPUT_FLUSH_THREAD_NAME = "OutputFlusher";

   private static final Logger LOG = LoggerFactory.getLogger(RecordWriter.class);
//底层的 ResultPartition
   protected final ResultPartitionWriter targetPartition;
//channel的数量，即 sub-partition的数量
   protected final int numberOfChannels;
//序列化
   protected final RecordSerializer<T> serializer;

   protected final Random rng = new XORShiftRandom();

   private Counter numBytesOut = new SimpleCounter();

   private Counter numBuffersOut = new SimpleCounter();

   private final boolean flushAlways;

   /** The thread that periodically flushes the output, to give an upper latency bound. */
   //定时强制 flush 输出buffer
   @Nullable
   private final OutputFlusher outputFlusher;

   /** To avoid synchronization overhead on the critical path, best-effort error tracking is enough here.*/
   private Throwable flusherException;

   RecordWriter(ResultPartitionWriter writer, long timeout, String taskName) {
      this.targetPartition = writer;
      this.numberOfChannels = writer.getNumberOfSubpartitions();
//序列化器，用于将一条记录序列化到多个buffer中
      this.serializer = new SpanningRecordSerializer<T>();

      checkArgument(timeout >= -1);
      this.flushAlways = (timeout == 0);
      if (timeout == -1 || timeout == 0) {
         outputFlusher = null;
      } else {
      //根据超时时间创建一个定时 flush 输出 buffer 的线程
         String threadName = taskName == null ?
            DEFAULT_OUTPUT_FLUSH_THREAD_NAME :
            DEFAULT_OUTPUT_FLUSH_THREAD_NAME + " for " + taskName;

         outputFlusher = new OutputFlusher(threadName, timeout);
         outputFlusher.start();
      }
   }
```

  buffer的分配在task启动的时候，调用ResultPartitionWriter的setup，

```
public static void setupPartitionsAndGates(
   ResultPartitionWriter[] producedPartitions, InputGate[] inputGates) throws IOException, InterruptedException {

   for (ResultPartitionWriter partition : producedPartitions) {
      partition.setup();
   }

   // InputGates must be initialized after the partitions, since during InputGate#setup
   // we are requesting partitions
   for (InputGate gate : inputGates) {
      gate.setup();
   }
}
```

ResultPartition的setup

```
public void setup() throws IOException {
   checkState(this.bufferPool == null, "Bug in result partition setup logic: Already registered buffer pool.");

   BufferPool bufferPool = checkNotNull(bufferPoolFactory.apply(this));
   //创建一个 LocalBufferPool，请求的最少的 MemeorySegment 数量和 sub-partition 一致
   checkArgument(bufferPool.getNumberOfRequiredMemorySegments() >= getNumberOfSubpartitions(),
      "Bug in result partition setup logic: Buffer pool has not enough guaranteed buffers for this result partition.");

   this.bufferPool = bufferPool;
   //向 ResultPartitionManager 注册
   partitionManager.registerResultPartition(this);
}
```

ResultPartitionManager

```
public void registerResultPartition(ResultPartition partition) {
   synchronized (registeredPartitions) {
      checkState(!isShutdown, "Result partition manager already shut down.");

      ResultPartition previous = registeredPartitions.put(partition.getPartitionId(), partition);

      if (previous != null) {
         throw new IllegalStateException("Result partition already registered.");
      }

      LOG.debug("Registered {}.", partition);
   }
}
```

当 Task 通过 `RecordWriter` 输出一条记录时，主要流程为：

1. 通过 ChannelSelector 确定写入的目标 channel

2. 使用 RecordSerializer 对记录进行序列化

3. 向 ResultPartition 请求 BufferBuilder，用于写入序列化结果

4. 向 ResultPartition 添加 BufferConsumer，用于读取写入 Buffer 的数据

   

   RecordWriter类 emit，前面调用recordWriter.emit会区分BroadcastRecordWriter类的emit和ChannelSelectorRecordWriter的emit，最终都会调用RecordWriter类 的emit

ChannelSelectorRecordWriter，选择channel发送

```
public final class ChannelSelectorRecordWriter<T extends IOReadableWritable> extends RecordWriter<T> {
//决定一条记录应该写入哪一个channel， 即 sub-partition
   private final ChannelSelector<T> channelSelector;
//供每一个 channel 写入数据使用
   /** Every subpartition maintains a separate buffer builder which might be null. */
   private final BufferBuilder[] bufferBuilders;

   ChannelSelectorRecordWriter(
         ResultPartitionWriter writer,
         ChannelSelector<T> channelSelector,
         long timeout,
         String taskName) {
      super(writer, timeout, taskName);

      this.channelSelector = checkNotNull(channelSelector);
      this.channelSelector.setup(numberOfChannels);

      this.bufferBuilders = new BufferBuilder[numberOfChannels];
   }

   @Override
   public void emit(T record) throws IOException, InterruptedException {
      emit(record, channelSelector.selectChannel(record));
   }
```

BroadcastRecordWriter，发送给所有channel

```
public final class BroadcastRecordWriter<T extends IOReadableWritable> extends RecordWriter<T> {

//供每一个 channel 写入数据使用
   /** The current buffer builder shared for all the channels. */
   @Nullable
   private BufferBuilder bufferBuilder;

   /**
    * The flag for judging whether {@link #requestNewBufferBuilder(int)} and {@link #flushTargetPartition(int)}
    * is triggered by {@link #randomEmit(IOReadableWritable)} or not.
    */
   private boolean randomTriggered;

   BroadcastRecordWriter(
         ResultPartitionWriter writer,
         long timeout,
         String taskName) {
      super(writer, timeout, taskName);
   }

   @Override
   public void emit(T record) throws IOException, InterruptedException {
      broadcastEmit(record);
   }
   
   public void broadcastEmit(T record) throws IOException, InterruptedException {
		// We could actually select any target channel here because all the channels
		// are sharing the same BufferBuilder in broadcast mode.
		emit(record, 0);
	}
```

RecordWriter类 的emit

```
protected void emit(T record, int targetChannel) throws IOException, InterruptedException {
      checkErroneous();
//序列化
      serializer.serializeRecord(record);

      // Make sure we don't hold onto the large intermediate serialization buffer for too long
      //将序列化结果写入buffer
       if (copyFromSerializerToTargetChannel(targetChannel)) {
       //清除序列化使用的buffer（这个是序列化时临时写入的byte[]）,减少内存占用
        serializer.prune();
      }
   }

   /**

   * @param targetChannel

     * @return <tt>true</tt> if the intermediate serialization buffer should be pruned
       */
       protected boolean copyFromSerializerToTargetChannel(int targetChannel) throws IOException, InterruptedException {
       // We should reset the initial position of the intermediate serialization buffer before
       // copying, so the serialization results can be copied to multiple target buffers.
       serializer.reset();

     boolean pruneTriggered = false;
     BufferBuilder bufferBuilder = getBufferBuilder(targetChannel);
     SerializationResult result = serializer.copyToBufferBuilder(bufferBuilder);
     while (result.isFullBuffer()) {
     //buffer 写满了，调用 bufferBuilder.finish 方法
        finishBufferBuilder(bufferBuilder);

        // If this was a full record, we are done. Not breaking out of the loop at this point
        // will lead to another buffer request before breaking out (that would not be a
        // problem per se, but it can lead to stalls in the pipeline).
        if (result.isFullRecord()) {
           pruneTriggered = true;
           //如果当前这条记录也完整输出了，清空bufferBuilder
           emptyCurrentBufferBuilder(targetChannel);
           break;
        }

//当前这条记录没有写完，申请新的 buffer 写入
        bufferBuilder = requestNewBufferBuilder(targetChannel);
        result = serializer.copyToBufferBuilder(bufferBuilder);
     }
     checkState(!serializer.hasSerializedData(), "All data should be written at once");

     if (flushAlways) {
     //强制刷新结果
        flushTargetPartition(targetChannel);
     }
     return pruneTriggered;
        }
```

```
protected void finishBufferBuilder(BufferBuilder bufferBuilder) {
   numBytesOut.inc(bufferBuilder.finish());
   numBuffersOut.inc();
}
```

```
ChannelSelectorRecordWriter：
public BufferBuilder getBufferBuilder(int targetChannel) throws IOException, InterruptedException {
   if (bufferBuilders[targetChannel] != null) {
      return bufferBuilders[targetChannel];
   } else {
      return requestNewBufferBuilder(targetChannel);
   }
}

BroadcastRecordWriter：
public BufferBuilder getBufferBuilder(int targetChannel) throws IOException, InterruptedException {
		return bufferBuilder != null ? bufferBuilder : requestNewBufferBuilder(targetChannel);
	}
```

//请求新的 BufferBuilder，用于写入数据 如果当前没有可用的 buffer，会阻塞

```
ChannelSelectorRecordWriter：
public BufferBuilder requestNewBufferBuilder(int targetChannel) throws IOException, InterruptedException {
		checkState(bufferBuilders[targetChannel] == null || bufferBuilders[targetChannel].isFinished());

//从 LocalBufferPool 中请求 BufferBuilder
		BufferBuilder bufferBuilder = targetPartition.getBufferBuilder();
		//添加一个BufferConsumer，用于读取写入到 MemorySegment 的数据
		targetPartition.addBufferConsumer(bufferBuilder.createBufferConsumer(), targetChannel);
		bufferBuilders[targetChannel] = bufferBuilder;
		return bufferBuilder;
	}


BroadcastRecordWriter：
public BufferBuilder requestNewBufferBuilder(int targetChannel) throws IOException, InterruptedException {
		checkState(bufferBuilder == null || bufferBuilder.isFinished());

		BufferBuilder builder = targetPartition.getBufferBuilder();
		if (randomTriggered) {
			targetPartition.addBufferConsumer(builder.createBufferConsumer(), targetChannel);
		} else {
			try (BufferConsumer bufferConsumer = builder.createBufferConsumer()) {
				for (int channel = 0; channel < numberOfChannels; channel++) {
					targetPartition.addBufferConsumer(bufferConsumer.copy(), channel);
				}
			}
		}

		bufferBuilder = builder;
		return builder;
	}
```

//从 LocalBufferPool 中请求 BufferBuilder调用ResultPartition的getBufferBuilder，ResultPartition实现了ResultPartitionWriter接口

```
public class ResultPartition implements ResultPartitionWriter, BufferPoolOwner {

   protected static final Logger LOG = LoggerFactory.getLogger(ResultPartition.class);

   private final String owningTaskName;

   protected final ResultPartitionID partitionId;

   /** Type of this partition. Defines the concrete subpartition implementation to use. */
   protected final ResultPartitionType partitionType;

   /** The subpartitions of this partition. At least one. */
   protected final ResultSubpartition[] subpartitions;

   protected final ResultPartitionManager partitionManager;

   public final int numTargetKeyGroups;

   // - Runtime state --------------------------------------------------------

   private final AtomicBoolean isReleased = new AtomicBoolean();

   private BufferPool bufferPool;

   private boolean isFinished;

   private volatile Throwable cause;

   private final FunctionWithException<BufferPoolOwner, BufferPool, IOException> bufferPoolFactory;
```

ResultPartition.getBufferBuilder()

```
public BufferBuilder getBufferBuilder() throws IOException, InterruptedException {
   checkInProduceState();

   return bufferPool.requestBufferBuilderBlocking();
}
```

LocalBufferPool .requestBufferBuilderBlocking,请求MemorySegment内存资源

```
@Override
public BufferBuilder requestBufferBuilderBlocking() throws IOException, InterruptedException {
   return toBufferBuilder(requestMemorySegmentBlocking());
}

private BufferBuilder toBufferBuilder(MemorySegment memorySegment) {
   if (memorySegment == null) {
      return null;
   }
   return new BufferBuilder(memorySegment, this);
}

private MemorySegment requestMemorySegmentBlocking() throws InterruptedException, IOException {
		MemorySegment segment;
		while ((segment = requestMemorySegment()) == null) {
			try {
				// wait until available
				getAvailableFuture().get();
			} catch (ExecutionException e) {
				LOG.error("The available future is completed exceptionally.", e);
				ExceptionUtils.rethrow(e);
			}
		}
		return segment;
	}
	
```

ResultPartition.addBufferConsumer()，向指定的 subpartition 添加一个 buffer

```
public boolean addBufferConsumer(BufferConsumer bufferConsumer, int subpartitionIndex) throws IOException {
   checkNotNull(bufferConsumer);

   ResultSubpartition subpartition;
   try {
      checkInProduceState();
      subpartition = subpartitions[subpartitionIndex];
   }
   catch (Exception ex) {
      bufferConsumer.close();
      throw ex;
   }

//添加 BufferConsumer，说明已经有数据生成了
   return subpartition.add(bufferConsumer);
}
```

ResultPartition转交给ResultSubpartition添加bufferConsumer，

```
public abstract class ResultSubpartition {

    /** The index of the subpartition at the parent partition. */
	protected final int index;

	/** The parent partition this subpartition belongs to. */
	protected final ResultPartition parent;

   public abstract boolean add(BufferConsumer bufferConsumer) throws IOException;
```

对于stream的任务，实现类是PipelinedSubpartition

```
class PipelinedSubpartition extends ResultSubpartition {

   private static final Logger LOG = LoggerFactory.getLogger(PipelinedSubpartition.class);

   // ------------------------------------------------------------------------

   /** All buffers of this subpartition. Access to the buffers is synchronized on this object. */
   //当前 subpartiion 堆积的所有的 Buffer 的队列
   private final ArrayDeque<BufferConsumer> buffers = new ArrayDeque<>();

   /** The number of non-event buffers currently in this subpartition. */
   @GuardedBy("buffers")
   //当前 subpartiion 中堆积的 buffer 的数量
   private int buffersInBacklog;

   /** The read view to consume this subpartition. */
   //用于消费写入的 Buffer
   private PipelinedSubpartitionView readView;

   /** Flag indicating whether the subpartition has been finished. */
   private boolean isFinished;

   @GuardedBy("buffers")
   private boolean flushRequested;

   /** Flag indicating whether the subpartition has been released. */
   private volatile boolean isReleased;

   /** The total number of buffers (both data and event buffers). */
   private long totalNumberOfBuffers;

   /** The total number of bytes (both data and event buffers). */
   private long totalNumberOfBytes;

   // ------------------------------------------------------------------------

//index 是当前 sub-paritition 的索引
   PipelinedSubpartition(int index, ResultPartition parent) {
      super(index, parent);
   }

   @Override
   public boolean add(BufferConsumer bufferConsumer) {
      return add(bufferConsumer, false);
   }

   @Override
   public void finish() throws IOException {
      add(EventSerializer.toBufferConsumer(EndOfPartitionEvent.INSTANCE), true);
      LOG.debug("{}: Finished {}.", parent.getOwningTaskName(), this);
   }

//添加一个新的BufferConsumer
	//这个参数里的 finish 指的是整个 subpartition 都完成了
   private boolean add(BufferConsumer bufferConsumer, boolean finish) {
      checkNotNull(bufferConsumer);

      final boolean notifyDataAvailable;
      synchronized (buffers) {
         if (isFinished || isReleased) {
            bufferConsumer.close();
            return false;
         }

         // Add the bufferConsumer and update the stats
         buffers.add(bufferConsumer);
         updateStatistics(bufferConsumer);
         //更新 backlog 的数量，只有 buffer 才会使得 buffersInBacklog + 1，事件不会增加 buffersInBacklog
         increaseBuffersInBacklog(bufferConsumer);
         notifyDataAvailable = shouldNotifyDataAvailable() || finish;

         isFinished |= finish;
      }

      if (notifyDataAvailable) {
      //通知数据可以被消费
         notifyDataAvailable();
      }

      return true;
   }
   
   //只在第一个 buffer 为 finish 的时候才通知
   private boolean shouldNotifyDataAvailable() {
		// Notify only when we added first finished buffer.
		return readView != null && !flushRequested && getNumberOfFinishedBuffers() == 1;
	}

//通知readView，有数据可用了
	private void notifyDataAvailable() {
		if (readView != null) {
			readView.notifyDataAvailable();
		}
	}
	
     public void flush() {
		final boolean notifyDataAvailable;
		synchronized (buffers) {
			if (buffers.isEmpty()) {
				return;
			}
			// if there is more then 1 buffer, we already notified the reader
			// (at the latest when adding the second buffer)
			notifyDataAvailable = !flushRequested && buffers.size() == 1 && buffers.peek().isDataAvailable();
			flushRequested = flushRequested || buffers.size() > 1 || notifyDataAvailable;
		}
		if (notifyDataAvailable) {
			notifyDataAvailable();
		}
	}
```

在强制进行 flush 的时候，也会发出数据可用的通知。这是因为，假如产出的数据记录较少无法完整地填充一个 `MemorySegment`，那么 `ResultSubpartition` 可能会一直处于不可被消费的状态。而为了保证产出的记录能够及时被消费，就需要及时进行 flush，从而确保下游能更及时地处理数据。在 `RecordWriter` 中有一个 `OutputFlusher` 会定时触发 flush，间隔可以通过 `DataStream.setBufferTimeout()` 来控制。

写入的 Buffer 最终被保存在 `ResultSubpartition` 中维护的一个队列中，如果需要消费这些 Buffer，就需要依赖 `ResultSubpartitionView`。当需要消费一个 `ResultSubpartition` 的结果时，需要创建一个 `ResultSubpartitionView` 对象，并关联到 `ResultSubpartition` 中；当数据可以被消费时，会通过对应的回调接口告知 `ResultSubpartitionView`

对于stream，PipelinedSubpartitionView实现了ResultSubpartitionView的接口

```
class PipelinedSubpartitionView implements ResultSubpartitionView {

   /** The subpartition this view belongs to. */
   private final PipelinedSubpartition parent;

   private final BufferAvailabilityListener availabilityListener;

   /** Flag indicating whether this view has been released. */
   private final AtomicBoolean isReleased;

   PipelinedSubpartitionView(PipelinedSubpartition parent, BufferAvailabilityListener listener) {
      this.parent = checkNotNull(parent);
      this.availabilityListener = checkNotNull(listener);
      this.isReleased = new AtomicBoolean();
   }

   @Nullable
   @Override
   public BufferAndBacklog getNextBuffer() {
      return parent.pollBuffer();
   }

   @Override
   public void notifyDataAvailable() {
   //回调接口
      availabilityListener.notifyDataAvailable();
   }
```

至此，我们已经了解了一个 Task 如何输出结果到 `ResultPartition` 中，以及如何去消费不同 `ResultSubpartition` 中的这些用于保存序列化结果的 Buffer。

继续看task的数据输入，availabilityListener.notifyDataAvailable()

```
public interface BufferAvailabilityListener {

   /**
    * Called whenever there might be new data available.
    */
   void notifyDataAvailable();
}
```

BufferAvailabilityListener有多种实现，如果一个 `InputChannel` 和其消费的上游 `ResultPartition` 所属 Task 都在同一个 TaskManager 中运行，那么它们之间的数据交换就在同一个 JVM 进程内不同线程之间进行，无需通过网络交换。也就是`LocalInputChannel`，LocalInputChannel继承自BufferAvailabilityListener

```
public class LocalInputChannel extends InputChannel implements BufferAvailabilityListener {  
   public void notifyDataAvailable() {
		notifyChannelNonEmpty();
	}
	
}
```

然后回调了InputChannel的notifyChannelNonEmpty

```
protected void notifyChannelNonEmpty() {
   inputGate.notifyChannelNonEmpty(this);
}
```

 `InputChannel` 和该 Task 需要消费的 `ResultSubpartition` 是一一对应的。

```
public abstract class InputChannel {

   protected final int channelIndex;

   protected final ResultPartitionID partitionId;

   protected final SingleInputGate inputGate;

   // - Asynchronous error notification --------------------------------------

   private final AtomicReference<Throwable> cause = new AtomicReference<Throwable>();

   // - Partition request backoff --------------------------------------------

   /** The initial backoff (in ms). */
   private final int initialBackoff;

   /** The maximum backoff (in ms). */
   private final int maxBackoff;

   protected final Counter numBytesIn;

   protected final Counter numBuffersIn;

   /** The current backoff (in ms). */
   private int currentBackoff;


   protected void notifyChannelNonEmpty() {
      inputGate.notifyChannelNonEmpty(this);
   }
```

再调用inputGate的notifyChannelNonEmpty，Task 的输入被抽象为 `InputGate`, 而 `InputGate` 则由 `InputChannel` 组成

```
public abstract class InputGate implements PullingAsyncDataInput<BufferOrEvent>, AutoCloseable {

   protected final AvailabilityHelper availabilityHelper = new AvailabilityHelper();

   public abstract int getNumberOfInputChannels();

   public abstract boolean isFinished();
阻塞调用
   public abstract Optional<BufferOrEvent> getNext() throws IOException, InterruptedException;
非阻塞调用
   public abstract Optional<BufferOrEvent> pollNext() throws IOException, InterruptedException;

   public abstract void sendTaskEvent(TaskEvent event) throws IOException;
```

InputGate的实现类是SingleInputGate，进入SingleInputGate的notifyChannelNonEmpty

```
public class SingleInputGate extends InputGate {

	//该 InputGate 包含的所有 InputChannel
	private final Map<IntermediateResultPartitionID, InputChannel> inputChannels;

	/** Channels, which notified this input gate about available data. */
	//InputChannel 构成的队列，这些 InputChannel 中都有有可供消费的数据
	private final ArrayDeque<InputChannel> inputChannelsWithData = new ArrayDeque<>();

	 //用于接收输入的缓冲池
	private BufferPool bufferPool;

    //当一个 InputChannel 有数据时的回调
    void notifyChannelNonEmpty(InputChannel channel) {
        queueChannel(checkNotNull(channel));
    }

    //将新的channel加入队列
    private void queueChannel(InputChannel channel) {
		int availableChannels;

		CompletableFuture<?> toNotify = null;

		synchronized (inputChannelsWithData) {
		//判断这个channel是否已经在队列中
			if (enqueuedInputChannelsWithData.get(channel.getChannelIndex())) {
				return;
			}
			availableChannels = inputChannelsWithData.size();

         //加入队列
			inputChannelsWithData.add(channel);
			enqueuedInputChannelsWithData.set(channel.getChannelIndex());

			if (availableChannels == 0) {
			//如果之前队列中没有channel，这个channel加入后，通知等待的线程
				inputChannelsWithData.notifyAll();
				toNotify = availabilityHelper.getUnavailableToResetAvailable();
			}
		}

		if (toNotify != null) {
			toNotify.complete(null);
		}
	}
	
	
	//在task启动的setupPartitionsAndGates里调用
@Override
	public void setup() throws IOException, InterruptedException {
		checkState(this.bufferPool == null, "Bug in input gate setup logic: Already registered buffer pool.");
		// assign exclusive buffers to input channels directly and use the rest for floating buffers
		assignExclusiveSegments();

		BufferPool bufferPool = bufferPoolFactory.get();
		setBufferPool(bufferPool);

		requestPartitions();
	}

	@VisibleForTesting
	void requestPartitions() throws IOException, InterruptedException {
		synchronized (requestLock) {
		//只请求一次
			if (!requestedPartitionsFlag) {
				if (closeFuture.isDone()) {
					throw new IllegalStateException("Already released.");
				}

				// Sanity checks
				if (numberOfInputChannels != inputChannels.size()) {
					throw new IllegalStateException(String.format(
						"Bug in input gate setup logic: mismatch between " +
						"number of total input channels [%s] and the currently set number of input " +
						"channels [%s].",
						inputChannels.size(),
						numberOfInputChannels));
				}

				for (InputChannel inputChannel : inputChannels.values()) {
				//每一个channel都请求对应的子分区
					inputChannel.requestSubpartition(consumedSubpartitionIndex);
				}
			}

			requestedPartitionsFlag = true;
		}
	}
```

回调之后开启了InputChannel的消费周期，requestSubpartition，getNextBuffer，releaseAllResources

```
abstract void requestSubpartition(int subpartitionIndex) throws IOException, InterruptedException;


abstract Optional<BufferAndAvailability> getNextBuffer() throws IOException, InterruptedException;

abstract void releaseAllResources() throws IOException;
```

在LocalInputChannel中的实现

```
//请求消费对应的子分区
void requestSubpartition(int subpartitionIndex) throws IOException, InterruptedException {

   boolean retriggerRequest = false;

   // The lock is required to request only once in the presence of retriggered requests.
   synchronized (requestLock) {
      checkState(!isReleased, "LocalInputChannel has been released already");

      if (subpartitionView == null) {
         LOG.debug("{}: Requesting LOCAL subpartition {} of partition {}.",
            this, subpartitionIndex, partitionId);

         try {
         //Local，无需网络通信，通过 ResultPartitionManager 创建一个 ResultSubpartitionView
					//LocalInputChannel 实现了 BufferAvailabilityListener
					//在有数据时会得到通知，notifyDataAvailable 会被调用，进而将当前 channel 加到 InputGate 的可用 Channel 队列中
            ResultSubpartitionView subpartitionView = partitionManager.createSubpartitionView(
               partitionId, subpartitionIndex, this);

            if (subpartitionView == null) {
               throw new IOException("Error requesting subpartition.");
            }

            // make the subpartition view visible
            this.subpartitionView = subpartitionView;

            // check if the channel was released in the meantime
            if (isReleased) {
               subpartitionView.releaseAllResources();
               this.subpartitionView = null;
            }
         } catch (PartitionNotFoundException notFound) {
            if (increaseBackoff()) {
               retriggerRequest = true;
            } else {
               throw notFound;
            }
         }
      }
   }

   // Do this outside of the lock scope as this might lead to a
   // deadlock with a concurrent release of the channel via the
   // input gate.
   if (retriggerRequest) {
      inputGate.retriggerPartitionRequest(partitionId.getPartitionId());
   }
}

//读取数据，借助 ResultSubparitionView 消费 ResultSubparition 中的数据
Optional<BufferAndAvailability> getNextBuffer() throws IOException, InterruptedException {
		checkError();

		ResultSubpartitionView subpartitionView = this.subpartitionView;
		if (subpartitionView == null) {
			// There is a possible race condition between writing a EndOfPartitionEvent (1) and flushing (3) the Local
			// channel on the sender side, and reading EndOfPartitionEvent (2) and processing flush notification (4). When
			// they happen in that order (1 - 2 - 3 - 4), flush notification can re-enqueue LocalInputChannel after (or
			// during) it was released during reading the EndOfPartitionEvent (2).
			if (isReleased) {
				return Optional.empty();
			}

			// this can happen if the request for the partition was triggered asynchronously
			// by the time trigger
			// would be good to avoid that, by guaranteeing that the requestPartition() and
			// getNextBuffer() always come from the same thread
			// we could do that by letting the timer insert a special "requesting channel" into the input gate's queue
			subpartitionView = checkAndWaitForSubpartitionView();
		}

    //通过 ResultSubparitionView 获取
		BufferAndBacklog next = subpartitionView.getNextBuffer();

		if (next == null) {
			if (subpartitionView.isReleased()) {
				throw new CancelTaskException("Consumed partition " + subpartitionView + " has been released.");
			} else {
				return Optional.empty();
			}
		}

		numBytesIn.inc(next.buffer().getSize());
		numBuffersIn.inc();
		return Optional.of(new BufferAndAvailability(next.buffer(), next.isMoreAvailable(), next.buffersInBacklog()));
	}
```

调用PipelinedSubpartitionView的getNextBuffer

```
@Nullable
   @Override
   public BufferAndBacklog getNextBuffer() {
      return parent.pollBuffer();
   }
```

```
BufferAndBacklog pollBuffer() {
   synchronized (buffers) {
      Buffer buffer = null;

      if (buffers.isEmpty()) {
         flushRequested = false;
      }

      while (!buffers.isEmpty()) {
         BufferConsumer bufferConsumer = buffers.peek();

         buffer = bufferConsumer.build();

         checkState(bufferConsumer.isFinished() || buffers.size() == 1,
            "When there are multiple buffers, an unfinished bufferConsumer can not be at the head of the buffers queue.");

         if (buffers.size() == 1) {
            // turn off flushRequested flag if we drained all of the available data
            flushRequested = false;
         }

         if (bufferConsumer.isFinished()) {
            buffers.pop().close();
            decreaseBuffersInBacklogUnsafe(bufferConsumer.isBuffer());
         }

         if (buffer.readableBytes() > 0) {
            break;
         }
         buffer.recycleBuffer();
         buffer = null;
         if (!bufferConsumer.isFinished()) {
            break;
         }
      }

      if (buffer == null) {
         return null;
      }

      updateStatistics(buffer);
      // Do not report last remaining buffer on buffers as available to read (assuming it's unfinished).
      // It will be reported for reading either on flush or when the number of buffers in the queue
      // will be 2 or more.
      return new BufferAndBacklog(
         buffer,
         isAvailableUnsafe(),
         getBuffersInBacklog(),
         nextBufferIsEventUnsafe());
   }
}
```

已上是task内部的数据传递，再看下task之间的数据传递，涉及到网络协议

在Task启动的时候，会调用inputgate的setup方法

```
public static void setupPartitionsAndGates(
   ResultPartitionWriter[] producedPartitions, InputGate[] inputGates) throws IOException, InterruptedException {

   for (ResultPartitionWriter partition : producedPartitions) {
      partition.setup();
   }

   // InputGates must be initialized after the partitions, since during InputGate#setup
   // we are requesting partitions
   for (InputGate gate : inputGates) {
      gate.setup();
   }
}
```

SingleInputGate

```
public void setup() throws IOException, InterruptedException {
   checkState(this.bufferPool == null, "Bug in input gate setup logic: Already registered buffer pool.");
   // assign exclusive buffers to input channels directly and use the rest for floating buffers
   assignExclusiveSegments();

//分配 LocalBufferPool 本地缓冲池，这是所有 channel 共享的
   BufferPool bufferPool = bufferPoolFactory.get();
   setBufferPool(bufferPool);

   requestPartitions();
}


public void assignExclusiveSegments() throws IOException {
		synchronized (requestLock) {
			for (InputChannel inputChannel : inputChannels.values()) {
				if (inputChannel instanceof RemoteInputChannel) {
				//RemoteInputChannel 请求独占的 buffer
					((RemoteInputChannel) inputChannel).assignExclusiveSegments();
				}
			}
		}
	}
	
void requestPartitions() throws IOException, InterruptedException {
		synchronized (requestLock) {
			if (!requestedPartitionsFlag) {
				if (closeFuture.isDone()) {
					throw new IllegalStateException("Already released.");
				}

				// Sanity checks
				if (numberOfInputChannels != inputChannels.size()) {
					throw new IllegalStateException(String.format(
						"Bug in input gate setup logic: mismatch between " +
						"number of total input channels [%s] and the currently set number of input " +
						"channels [%s].",
						inputChannels.size(),
						numberOfInputChannels));
				}

				for (InputChannel inputChannel : inputChannels.values()) {
					inputChannel.requestSubpartition(consumedSubpartitionIndex);
				}
			}

			requestedPartitionsFlag = true;
		}
	}
```

RemoteInputChannel 管理可用 buffer

在 `RemoteInputChannel` 内部使用 `AvailableBufferQueue` 来管理所有可用的 buffer：

```
//分配独占的 buffer
void assignExclusiveSegments() throws IOException {
   checkState(initialCredit == 0, "Bug in input channel setup logic: exclusive buffers have " +
      "already been set for this input channel.");

   Collection<MemorySegment> segments = checkNotNull(memorySegmentProvider.requestMemorySegments());
   checkArgument(!segments.isEmpty(), "The number of exclusive buffers per channel should be larger than 0.");

//初始的Credit
   initialCredit = segments.size();
   numRequiredBuffers = segments.size();

   synchronized (bufferQueue) {
      for (MemorySegment segment : segments) {
      //注意这个 NetworkBuffer 的回收器是 RemoteInputChannel 自身
         bufferQueue.addExclusiveBuffer(new NetworkBuffer(segment, this), numRequiredBuffers);
      }
   }
}
```

AvailableBufferQueue

```
private static class AvailableBufferQueue {
//添加一个独占的buffer，如果当前可用的 buffer 总量超出了要求的数量，则向本地缓冲池归还一个流动的buffer
		//返回值是新增的 buffer 数量
   int addExclusiveBuffer(Buffer buffer, int numRequiredBuffers) {
			exclusiveBuffers.add(buffer);
			if (getAvailableBufferSize() > numRequiredBuffers) {
				Buffer floatingBuffer = floatingBuffers.poll();
				floatingBuffer.recycleBuffer();
				//加一个，归还一个，相当于没加
				return 0;
			} else {
				return 1;
			}
		}
		
	
}
```

NetworkBuffer

```
public void recycleBuffer() {
//最终调用deallocate
   release();
}

protected void deallocate() {
//调用RemoteInputChannel 的recycle
		recycler.recycle(memorySegment);
	}
```

RemoteInputChannel.recycle

```
//独占的 buffer 释放后会直接被 RemoteInputChannel 回收
public void recycle(MemorySegment segment) {
   int numAddedBuffers;

   synchronized (bufferQueue) {
      // Similar to notifyBufferAvailable(), make sure that we never add a buffer
      // after releaseAllResources() released all buffers (see below for details).
      if (isReleased.get()) {
         try {
            //如果这个 channle 已经被释放，这个 MemorySegment 会被归还给 NetworkBufferPoolmemorySegmentProvider.recycleMemorySegments(Collections.singletonList(segment));
            return;
         } catch (Throwable t) {
            ExceptionUtils.rethrow(t);
         }
      }
      //重新加入到 AvailableBufferQueue 中
      numAddedBuffers = bufferQueue.addExclusiveBuffer(new NetworkBuffer(segment, this), numRequiredBuffers);
   }

   if (numAddedBuffers > 0 && unannouncedCredit.getAndAdd(numAddedBuffers) == 0) {
      notifyCreditAvailable();
   }
}
```

前面SingleInputGate的requestPartitions方法里会调用inputChannel.requestSubpartition请求远端子分区，在RemoteInputChannel中的实现如下，会创建一个 `PartitionRequestClient`，并通过 Netty 发送 `PartitionRequest` 请求，这时会带上当前 InputChannel 的 id 和初始的 credit 信息

```
public void requestSubpartition(int subpartitionIndex) throws IOException, InterruptedException {
//REMOTE，需要网络通信，使用 Netty 建立网络
//通过 ConnectionManager 来建立连接：创建 PartitionRequestClient，通过 PartitionRequestClient 发起请求
   if (partitionRequestClient == null) {
      // Create a client and request the partition
      try {
         partitionRequestClient = connectionManager.createPartitionRequestClient(connectionId);
      } catch (IOException e) {
         // IOExceptions indicate that we could not open a connection to the remote TaskExecutor
         throw new PartitionConnectionException(partitionId, e);
      }

//请求分区，通过 netty 发起请求
      partitionRequestClient.requestSubpartition(partitionId, subpartitionIndex, this, 0);
   }
}
```

NettyConnectionManager

```
public PartitionRequestClient createPartitionRequestClient(ConnectionID connectionId)
      throws IOException, InterruptedException {
      //这里实际上会建立和其它 Task 的 Server 的连接
		//返回的 PartitionRequestClient 中封装了 netty channel 和 channel handler
   return partitionRequestClientFactory.createPartitionRequestClient(connectionId);
}
```

PartitionRequestClientFactory

```
NettyPartitionRequestClient createPartitionRequestClient(ConnectionID connectionId) throws IOException, InterruptedException {
   Object entry;
   NettyPartitionRequestClient client = null;

   while (client == null) {
      entry = clients.get(connectionId);

      if (entry != null) {
      //连接已经建立
         // Existing channel or connecting channel
         if (entry instanceof NettyPartitionRequestClient) {
            client = (NettyPartitionRequestClient) entry;
         }
         else {
            ConnectingChannel future = (ConnectingChannel) entry;
            client = future.waitForChannel();

            clients.replace(connectionId, future, client);
         }
      }
      else {
         // No channel yet. Create one, but watch out for a race.
         // We create a "connecting future" and atomically add it to the map.
         // Only the thread that really added it establishes the channel.
         // The others need to wait on that original establisher's future.
         // 连接创建成功后会回调 handInChannel 方法
         ConnectingChannel connectingChannel = new ConnectingChannel(connectionId, this);
         Object old = clients.putIfAbsent(connectionId, connectingChannel);

         if (old == null) {
         //连接到 Netty Server
          nettyClient.connect(connectionId.getAddress()).addListener(connectingChannel);

            client = connectingChannel.waitForChannel();

            clients.replace(connectionId, connectingChannel, client);
         }
         else if (old instanceof ConnectingChannel) {
            client = ((ConnectingChannel) old).waitForChannel();
            clients.replace(connectionId, old, client);
         }
         else {
            client = (NettyPartitionRequestClient) old;
         }
      }

      // Make sure to increment the reference count before handing a client
      // out to ensure correct bookkeeping for channel closing.
      if (!client.incrementReferenceCounter()) {
         destroyPartitionRequestClient(connectionId, client);
         client = null;
      }
   }

   return client;
}
```

创建了PartitionRequestClient之后，发送请求，调用partitionRequestClient的requestSubpartition，PartitionRequestClient接口的实现类是NettyPartitionRequestClient，进入NettyPartitionRequestClient的requestSubpartition

```
public void requestSubpartition(
      final ResultPartitionID partitionId,
      final int subpartitionIndex,
      final RemoteInputChannel inputChannel,
      int delayMs) throws IOException {

   checkNotClosed();

   LOG.debug("Requesting subpartition {} of partition {} with {} ms delay.",
         subpartitionIndex, partitionId, delayMs);
//向 NetworkClientHandler 注册当前 RemoteInputChannel
		//单个 Task 所有的 RemoteInputChannel 的数据传输都通过这个 PartitionRequestClient 处理
   clientHandler.addInputChannel(inputChannel);

//PartitionRequest封装了请求的 sub-partition 的信息，当前 input channel 的 ID，以及初始 credit
   final PartitionRequest request = new PartitionRequest(
         partitionId, subpartitionIndex, inputChannel.getInputChannelId(), inputChannel.getInitialCredit());

//请求的监听
   final ChannelFutureListener listener = new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
         if (!future.isSuccess()) {
         //如果请求发送失败，要移除当前的 inputChannel
            clientHandler.removeInputChannel(inputChannel);
            SocketAddress remoteAddr = future.channel().remoteAddress();
            inputChannel.onError(
                  new LocalTransportException(
                     String.format("Sending the partition request to '%s' failed.", remoteAddr),
                     future.channel().localAddress(), future.cause()
                  ));
         }
      }
   };

//通过 netty 发送请求
   if (delayMs == 0) {
      ChannelFuture f = tcpChannel.writeAndFlush(request);
      f.addListener(listener);
   } else {
      final ChannelFuture[] f = new ChannelFuture[1];
      tcpChannel.eventLoop().schedule(new Runnable() {
         @Override
         public void run() {
            f[0] = tcpChannel.writeAndFlush(request);
            f[0].addListener(listener);
         }
      }, delayMs, TimeUnit.MILLISECONDS);
   }
}
```



接下来是生产者和消费者各自的处理流程

生产者端即 `ResultSubpartition` 一侧，在网络通信中对应 `NettyServer`。`NettyServer` 有两个重要的 `ChannelHandler`，即 `PartitionRequestServerHandler` 和 `PartitionRequestQueue`。其中，`PartitionRequestServerHandler` 负责处理消费端通过 `PartitionRequestClient` 发送的 `PartitionRequest` 和 `AddCredit` 等请求；`PartitionRequestQueue` 则包含了一个可以从中读取数据的 `NetworkSequenceViewReader` 队列，它会监听 Netty Channel 的可写入状态，一旦可以写入数据，就会从 `NetworkSequenceViewReader` 消费数据写入 Netty Channel。

消费端即 `RemoteInputChannel` 一侧，在网络通信中对应 `NettyClient`

而`NettyServer`和`NettyClient`是在启动TaskManager的startTaskManager过程中创建的，TaskManagerServices.fromConfiguration最终调用NettyConnectionManager的start

```
public int start() throws IOException {
//初始化 Netty Client
   client.init(nettyProtocol, bufferPool);
//初始化并启动 Netty Server
   return server.init(nettyProtocol, bufferPool);
}
```

NettyClient：

```
void init(final NettyProtocol protocol, NettyBufferPool nettyBufferPool) throws IOException {
   checkState(bootstrap == null, "Netty client has already been initialized.");

   this.protocol = protocol;

   final long start = System.nanoTime();

   bootstrap = new Bootstrap();

   // --------------------------------------------------------------------
   // Transport-specific configuration
   // --------------------------------------------------------------------

   switch (config.getTransportType()) {
      case NIO:
         initNioBootstrap();
         break;

      case EPOLL:
         initEpollBootstrap();
         break;

      case AUTO:
         if (Epoll.isAvailable()) {
            initEpollBootstrap();
            LOG.info("Transport type 'auto': using EPOLL.");
         }
         else {
            initNioBootstrap();
            LOG.info("Transport type 'auto': using NIO.");
         }
   }

   // --------------------------------------------------------------------
   // Configuration
   // --------------------------------------------------------------------

   bootstrap.option(ChannelOption.TCP_NODELAY, true);
   bootstrap.option(ChannelOption.SO_KEEPALIVE, true);

   // Timeout for new connections
   bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getClientConnectTimeoutSeconds() * 1000);

   // Pooled allocator for Netty's ByteBuf instances
   bootstrap.option(ChannelOption.ALLOCATOR, nettyBufferPool);

   // Receive and send buffer size
   int receiveAndSendBufferSize = config.getSendAndReceiveBufferSize();
   if (receiveAndSendBufferSize > 0) {
      bootstrap.option(ChannelOption.SO_SNDBUF, receiveAndSendBufferSize);
      bootstrap.option(ChannelOption.SO_RCVBUF, receiveAndSendBufferSize);
   }

   try {
      clientSSLFactory = config.createClientSSLEngineFactory();
   } catch (Exception e) {
      throw new IOException("Failed to initialize SSL Context for the Netty client", e);
   }

   final long duration = (System.nanoTime() - start) / 1_000_000;
   LOG.info("Successful initialization (took {} ms).", duration);
}
```

NettyServer：

```
int init(final NettyProtocol protocol, NettyBufferPool nettyBufferPool) throws IOException {
   return init(
      nettyBufferPool,
      sslHandlerFactory -> new ServerChannelInitializer(protocol, sslHandlerFactory));
}

int init(
      NettyBufferPool nettyBufferPool,
      Function<SSLHandlerFactory, ServerChannelInitializer> channelInitializer) throws IOException {
   checkState(bootstrap == null, "Netty server has already been initialized.");

   final long start = System.nanoTime();

   bootstrap = new ServerBootstrap();

   // --------------------------------------------------------------------
   // Transport-specific configuration
   // --------------------------------------------------------------------

   switch (config.getTransportType()) {
      case NIO:
         initNioBootstrap();
         break;

      case EPOLL:
         initEpollBootstrap();
         break;

      case AUTO:
         if (Epoll.isAvailable()) {
            initEpollBootstrap();
            LOG.info("Transport type 'auto': using EPOLL.");
         }
         else {
            initNioBootstrap();
            LOG.info("Transport type 'auto': using NIO.");
         }
   }

   // --------------------------------------------------------------------
   // Configuration
   // --------------------------------------------------------------------

   // Server bind address
   bootstrap.localAddress(config.getServerAddress(), config.getServerPort());

   // Pooled allocators for Netty's ByteBuf instances
   bootstrap.option(ChannelOption.ALLOCATOR, nettyBufferPool);
   bootstrap.childOption(ChannelOption.ALLOCATOR, nettyBufferPool);

   if (config.getServerConnectBacklog() > 0) {
      bootstrap.option(ChannelOption.SO_BACKLOG, config.getServerConnectBacklog());
   }

   // Receive and send buffer size
   int receiveAndSendBufferSize = config.getSendAndReceiveBufferSize();
   if (receiveAndSendBufferSize > 0) {
      bootstrap.childOption(ChannelOption.SO_SNDBUF, receiveAndSendBufferSize);
      bootstrap.childOption(ChannelOption.SO_RCVBUF, receiveAndSendBufferSize);
   }

   // Low and high water marks for flow control
   // hack around the impossibility (in the current netty version) to set both watermarks at
   // the same time:
   final int defaultHighWaterMark = 64 * 1024; // from DefaultChannelConfig (not exposed)
   final int newLowWaterMark = config.getMemorySegmentSize() + 1;
   final int newHighWaterMark = 2 * config.getMemorySegmentSize();
   //配置水位线，确保不往网络中写入太多数据
		//当输出缓冲中的字节数超过高水位值, 则 Channel.isWritable() 会返回false
		//当输出缓存中的字节数低于低水位值, 则 Channel.isWritable() 会重新返回true
		//背压就是这么产生的
   if (newLowWaterMark > defaultHighWaterMark) {
      bootstrap.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, newHighWaterMark);
      bootstrap.childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, newLowWaterMark);
   } else { // including (newHighWaterMark < defaultLowWaterMark)
      bootstrap.childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, newLowWaterMark);
      bootstrap.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, newHighWaterMark);
   }

   // SSL related configuration
   final SSLHandlerFactory sslHandlerFactory;
   try {
      sslHandlerFactory = config.createServerSSLEngineFactory();
   } catch (Exception e) {
      throw new IOException("Failed to initialize SSL Context for the Netty Server", e);
   }

   // --------------------------------------------------------------------
   // Child channel pipeline for accepted connections
   // --------------------------------------------------------------------

   bootstrap.childHandler(channelInitializer.apply(sslHandlerFactory));

   // --------------------------------------------------------------------
   // Start Server
   // --------------------------------------------------------------------

   bindFuture = bootstrap.bind().syncUninterruptibly();

   localAddress = (InetSocketAddress) bindFuture.channel().localAddress();

   final long duration = (System.nanoTime() - start) / 1_000_000;
   LOG.info("Successful initialization (took {} ms). Listening on SocketAddress {}.", duration, localAddress);

   return localAddress.getPort();
}
```

而参数NettyProtocol类提供了 `NettyClient` 和 `NettyServer` 引导启动注册的一系列 Channel Handler

```
public class NettyProtocol {

   private final NettyMessage.NettyMessageEncoder
      messageEncoder = new NettyMessage.NettyMessageEncoder();

   private final ResultPartitionProvider partitionProvider;
   private final TaskEventPublisher taskEventPublisher;

   NettyProtocol(ResultPartitionProvider partitionProvider, TaskEventPublisher taskEventPublisher) {
      this.partitionProvider = partitionProvider;
      this.taskEventPublisher = taskEventPublisher;
   }

   /**
    * Returns the server channel handlers.
    *
    * <pre>
    * +-------------------------------------------------------------------+
    * |                        SERVER CHANNEL PIPELINE                    |
    * |                                                                   |
    * |    +----------+----------+ (3) write  +----------------------+    |
    * |    | Queue of queues     +----------->| Message encoder      |    |
    * |    +----------+----------+            +-----------+----------+    |
    * |              /|\                                 \|/              |
    * |               | (2) enqueue                       |               |
    * |    +----------+----------+                        |               |
    * |    | Request handler     |                        |               |
    * |    +----------+----------+                        |               |
    * |              /|\                                  |               |
    * |               |                                   |               |
    * |   +-----------+-----------+                       |               |
    * |   | Message+Frame decoder |                       |               |
    * |   +-----------+-----------+                       |               |
    * |              /|\                                  |               |
    * +---------------+-----------------------------------+---------------+
    * |               | (1) client request               \|/
    * +---------------+-----------------------------------+---------------+
    * |               |                                   |               |
    * |       [ Socket.read() ]                    [ Socket.write() ]     |
    * |                                                                   |
    * |  Netty Internal I/O Threads (Transport Implementation)            |
    * +-------------------------------------------------------------------+
    * </pre>
    *
    * @return channel handlers
    */
   public ChannelHandler[] getServerChannelHandlers() {
   //netty server 端的 ChannelHandler
      PartitionRequestQueue queueOfPartitionQueues = new PartitionRequestQueue();
      PartitionRequestServerHandler serverHandler = new PartitionRequestServerHandler(
         partitionProvider,
         taskEventPublisher,
         queueOfPartitionQueues);

      return new ChannelHandler[] {
         messageEncoder,
         new NettyMessage.NettyMessageDecoder(),
         serverHandler,
         queueOfPartitionQueues
      };
   }

   /**
    * Returns the client channel handlers.
    *
    * <pre>
    *     +-----------+----------+            +----------------------+
    *     | Remote input channel |            | request client       |
    *     +-----------+----------+            +-----------+----------+
    *                 |                                   | (1) write
    * +---------------+-----------------------------------+---------------+
    * |               |     CLIENT CHANNEL PIPELINE       |               |
    * |               |                                  \|/              |
    * |    +----------+----------+            +----------------------+    |
    * |    | Request handler     +            | Message encoder      |    |
    * |    +----------+----------+            +-----------+----------+    |
    * |              /|\                                 \|/              |
    * |               |                                   |               |
    * |    +----------+------------+                      |               |
    * |    | Message+Frame decoder |                      |               |
    * |    +----------+------------+                      |               |
    * |              /|\                                  |               |
    * +---------------+-----------------------------------+---------------+
    * |               | (3) server response              \|/ (2) client request
    * +---------------+-----------------------------------+---------------+
    * |               |                                   |               |
    * |       [ Socket.read() ]                    [ Socket.write() ]     |
    * |                                                                   |
    * |  Netty Internal I/O Threads (Transport Implementation)            |
    * +-------------------------------------------------------------------+
    * </pre>
    *
    * @return channel handlers
    */
   public ChannelHandler[] getClientChannelHandlers() {
   //netty client 端的 ChannelHandler
      return new ChannelHandler[] {
         messageEncoder,
         new NettyMessage.NettyMessageDecoder(),
         new CreditBasedPartitionRequestClientHandler()};
   }

}
```

![](bpSourceCodeSending.png)

首先，当 `NettyServer` 接收到 `PartitionRequest` 消息后，`PartitionRequestServerHandler` 会创建一个 `NetworkSequenceViewReader` 对象，请求创建 `ResultSubpartitionView`, 并将 `NetworkSequenceViewReader` 保存在 `PartitionRequestQueue` 中。`PartitionRequestQueue` 会持有所有请求消费数据的 `RemoteInputChannel` 的 ID 和 `NetworkSequenceViewReader` 之间的映射关系。

我们已经知道，`ResultSubpartitionView` 用来消费 `ResultSubpartition` 中的数据，并在 `ResultSubpartition` 中有数据可用时获得提醒；`NetworkSequenceViewReader` 则相当于对 `ResultSubpartition` 的一层包装，她会按顺序为读取的每一个 buffer 分配一个序列号，并且记录了接收数据的 `RemoteInputChannel` 的 ID。在使用 Credit-based Flow Control 的情况下，`NetworkSequenceViewReader` 的具体实现对应为 `CreditBasedSequenceNumberingViewReader`。 `CreditBasedSequenceNumberingViewReader` 同时还实现了 `BufferAvailabilityListener` 接口，因而可以作为 `PipelinedSubpartitionView` 的回调对象。

```
class PartitionRequestServerHandler extends SimpleChannelInboundHandler<NettyMessage> {
    protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
		try {
			Class<?> msgClazz = msg.getClass();

			// ----------------------------------------------------------------
			// Intermediate result partition requests
			// ----------------------------------------------------------------
			if (msgClazz == PartitionRequest.class) {
			//Server 端接收到 client 发送的 PartitionRequest
				PartitionRequest request = (PartitionRequest) msg;

				LOG.debug("Read channel on {}: {}.", ctx.channel().localAddress(), request);

				try {
					NetworkSequenceViewReader reader;
					reader = new CreditBasedSequenceNumberingViewReader(
						request.receiverId,
						request.credit,
						outboundQueue);

//通过 ResultPartitionProvider（实际上就是 ResultPartitionManager）创建 ResultSubpartitionView
					//在有可被消费的数据产生后，PartitionRequestQueue.notifyReaderNonEmpty 会被回调，进而在 netty channelPipeline 上触发一次 fireUserEventTriggered
					reader.requestSubpartitionView(
						partitionProvider,
						request.partitionId,
						request.queueIndex);
//通知 PartitionRequestQueue 创建了一个 NetworkSequenceViewReader
					outboundQueue.notifyReaderCreated(reader);
				} catch (PartitionNotFoundException notFound) {
					respondWithError(ctx, notFound, request.receiverId);
				}
			}
			// ----------------------------------------------------------------
			// Task events
			// ----------------------------------------------------------------
			else if (msgClazz == TaskEventRequest.class) {
				TaskEventRequest request = (TaskEventRequest) msg;

				if (!taskEventPublisher.publish(request.partitionId, request.event)) {
					respondWithError(ctx, new IllegalArgumentException("Task event receiver not found."), request.receiverId);
				}
			} else if (msgClazz == CancelPartitionRequest.class) {
				CancelPartitionRequest request = (CancelPartitionRequest) msg;

				outboundQueue.cancel(request.receiverId);
			} else if (msgClazz == CloseRequest.class) {
				outboundQueue.close();
			} else if (msgClazz == AddCredit.class) {
				AddCredit request = (AddCredit) msg;

				outboundQueue.addCredit(request.receiverId, request.credit);
			} else {
				LOG.warn("Received unexpected client request: {}", msg);
			}
		} catch (Throwable t) {
			respondWithError(ctx, t);
		}
	}
```

CreditBasedSequenceNumberingViewReader：

```
class CreditBasedSequenceNumberingViewReader implements BufferAvailabilityListener, NetworkSequenceViewReader {

   private final Object requestLock = new Object();

//对应的 RemoteInputChannel 的 ID
   private final InputChannelID receiverId;

   private final PartitionRequestQueue requestQueue;
//消费 ResultSubpartition 的数据，并在 ResultSubpartition 有数据可用时获得通知
   private volatile ResultSubpartitionView subpartitionView;
//numCreditsAvailable的值是消费端还能够容纳的buffer的数量，也就是允许生产端发送的buffer的数量
   /** The number of available buffers for holding data on the consumer side. */
   private int numCreditsAvailable;
 //序列号，自增
   private int sequenceNumber = -1;
//创建一个 ResultSubpartitionView，用于读取数据，并在有数据可用时获得通知
   CreditBasedSequenceNumberingViewReader(
         InputChannelID receiverId,
         int initialCredit,
         PartitionRequestQueue requestQueue) {

      this.receiverId = receiverId;
      this.numCreditsAvailable = initialCredit;
      this.requestQueue = requestQueue;
   }

   @Override
   public void requestSubpartitionView(
      ResultPartitionProvider partitionProvider,
      ResultPartitionID resultPartitionId,
      int subPartitionIndex) throws IOException {

      synchronized (requestLock) {
         if (subpartitionView == null) {
            // This this call can trigger a notification we have to
            // schedule a separate task at the event loop that will
            // start consuming this. Otherwise the reference to the
            // view cannot be available in getNextBuffer().
            this.subpartitionView = partitionProvider.createSubpartitionView(
               resultPartitionId,
               subPartitionIndex,
               this);
         } else {
            throw new IllegalStateException("Subpartition already requested");
         }
      }
   }
   
   //当PartitionRequestQueue的WriteAndFlushNextMessageIfPossibleListener监听到写入数据时通过writeAndFlushNextMessageIfPossible调用getNextBuffer获取数据
   @Override
	public BufferAndAvailability getNextBuffer() throws IOException, InterruptedException {
		BufferAndBacklog next = subpartitionView.getNextBuffer();
		if (next != null) {
		//序列号
			sequenceNumber++;

			if (next.buffer().isBuffer() && --numCreditsAvailable < 0) {
			//要发送一个buffer，对应的 numCreditsAvailable 要减 1
				throw new IllegalStateException("no credit available");
			}

			return new BufferAndAvailability(
				next.buffer(), isAvailable(next), next.buffersInBacklog());
		} else {
			return null;
		}
	}
	
	
	//是否还可以消费数据：
	// 1. ResultSubpartition 中有更多的数据
	// 2. credit > 0 或者下一条数据是事件(事件不需要消耗credit)
	private boolean isAvailable(BufferAndBacklog bufferAndBacklog) {
		// BEWARE: this must be in sync with #isAvailable()!
		if (numCreditsAvailable > 0) {
			return bufferAndBacklog.isMoreAvailable();
		}
		else {
			return bufferAndBacklog.nextBufferIsEvent();
		}
	}
	
	//在 ResultSubparition 中有数据时会回调该方法
	@Override
	public void notifyDataAvailable() {
	//告知 PartitionRequestQueue 当前 ViewReader 有数据可读
		requestQueue.notifyReaderNonEmpty(this);
	}
```

`PartitionRequestQueue` 负责将 `ResultSubparition` 中的数据通过网络发送给 `RemoteInputChannel`。在 `PartitionRequestQueue` 中保存了所有的 `NetworkSequenceViewReader` 和 `InputChannelID` 之间的映射关系，以及一个 `ArrayDeque availableReaders` 队列。当一个 `NetworkSequenceViewReader` 中有数据可以被消费时，就会被加入到 `availableReaders` 队列中。

```
class PartitionRequestQueue extends ChannelInboundHandlerAdapter {

   private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestQueue.class);

   private final ChannelFutureListener writeListener = new WriteAndFlushNextMessageIfPossibleListener();

   /** The readers which are already enqueued available for transferring data. */
   private final ArrayDeque<NetworkSequenceViewReader> availableReaders = new ArrayDeque<>();

   /** All the readers created for the consumers' partition requests. */
   private final ConcurrentMap<InputChannelID, NetworkSequenceViewReader> allReaders = new ConcurrentHashMap<>();

   private boolean fatalError;

   private ChannelHandlerContext ctx;
   
   //添加新的 NetworkSequenceViewReader
   public void notifyReaderCreated(final NetworkSequenceViewReader reader) {
		allReaders.put(reader.getReceiverId(), reader);
	}

//通知 NetworkSequenceViewReader 有数据可读取
   void notifyReaderNonEmpty(final NetworkSequenceViewReader reader) {
      // The notification might come from the same thread. For the initial writes this
      // might happen before the reader has set its reference to the view, because
      // creating the queue and the initial notification happen in the same method call.
      // This can be resolved by separating the creation of the view and allowing
      // notifications.

      // TODO This could potentially have a bad performance impact as in the
      // worst case (network consumes faster than the producer) each buffer
      // will trigger a separate event loop task being scheduled.
      //触发一次用户自定义事件
      ctx.executor().execute(() -> ctx.pipeline().fireUserEventTriggered(reader));
   }
   
   //fireUserEventTriggered最终调用userEventTriggered
   @Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object msg) throws Exception {
		// The user event triggered event loop callback is used for thread-safe
		// hand over of reader queues and cancelled producers.

		if (msg instanceof NetworkSequenceViewReader) {
		//NetworkSequenceViewReader有数据可读取，加入队列中
			enqueueAvailableReader((NetworkSequenceViewReader) msg);
		} else if (msg.getClass() == InputChannelID.class) {
			// Release partition view that get a cancel request.
			// 对应的 RemoteInputChannel 请求取消消费
			InputChannelID toCancel = (InputChannelID) msg;

			// remove reader from queue of available readers
			availableReaders.removeIf(reader -> reader.getReceiverId().equals(toCancel));

			// remove reader from queue of all readers and release its resource
			final NetworkSequenceViewReader toRelease = allReaders.remove(toCancel);
			if (toRelease != null) {
				releaseViewReader(toRelease);
			}
		} else {
			ctx.fireUserEventTriggered(msg);
		}
	}
	
	//加入队列
	private void enqueueAvailableReader(final NetworkSequenceViewReader reader) throws Exception {
		if (reader.isRegisteredAsAvailable() || !reader.isAvailable()) {
		//已经被注册到队列中，或者暂时没有 buffer 或没有 credit 可用
			return;
		}
		// Queue an available reader for consumption. If the queue is empty,
		// we try trigger the actual write. Otherwise this will be handled by
		// the writeAndFlushNextMessageIfPossible calls.
		boolean triggerWrite = availableReaders.isEmpty();
		registerAvailableReader(reader);

		if (triggerWrite) {
		//如果这是队列中第一个元素，调用 writeAndFlushNextMessageIfPossible 发送数据
			writeAndFlushNextMessageIfPossible(ctx.channel());
		}
	}
```

`PartitionRequestQueue` 会监听 Netty Channel 的可写入状态，当 Channel 可写入时，就会从 `availableReaders` 队列中取出 `NetworkSequenceViewReader`，读取数据并写入网络。可写入状态是 Netty 通过水位线进行控制的，`NettyServer` 在启动的时候会配置水位线，如果 Netty 输出缓冲中的字节数超过了高水位值，我们会等到其降到低水位值以下才继续写入数据。通过水位线机制确保不往网络中写入太多数据。

```
private void writeAndFlushNextMessageIfPossible(final Channel channel) throws IOException {
   if (fatalError || !channel.isWritable()) {
   //如果当前不可写入，则直接返回
      return;
   }

   // The logic here is very similar to the combined input gate and local
   // input channel logic. You can think of this class acting as the input
   // gate and the consumed views as the local input channels.

   BufferAndAvailability next = null;
   try {
      while (true) {
      //取出一个 reader
         NetworkSequenceViewReader reader = pollAvailableReader();

         // No queue with available data. We allow this here, because
         // of the write callbacks that are executed after each write.
         if (reader == null) {
            return;
         }

         next = reader.getNextBuffer();
         if (next == null) {
         //没有读到数据
            if (!reader.isReleased()) {
            //还没有释放当前 reader，继续处理下一个 数据
               continue;
            }
            //出错了
            Throwable cause = reader.getFailureCause();
            if (cause != null) {
               ErrorResponse msg = new ErrorResponse(
                  new ProducerFailedException(cause),
                  reader.getReceiverId());

               ctx.writeAndFlush(msg);
            }
         } else {
         // 读到了数据
            // This channel was now removed from the available reader queue.
            // We re-add it into the queue if it is still available
            if (next.moreAvailable()) {
            //这个 reader 还可以读到更多的数据，继续加入队列
               registerAvailableReader(reader);
            }

            BufferResponse msg = new BufferResponse(
               next.buffer(),
               reader.getSequenceNumber(),
               reader.getReceiverId(),
               next.buffersInBacklog());

            // Write and flush and wait until this is done before
            // trying to continue with the next buffer.
            // 向 client 发送数据，发送成功之后通过 writeListener 的回调触发下一次发送
            channel.writeAndFlush(msg).addListener(writeListener);

            return;
         }
      }
   } catch (Throwable t) {
      if (next != null) {
         next.buffer().recycleBuffer();
      }

      throw new IOException(t.getMessage(), t);
   }
}

//继续加入队列
private void registerAvailableReader(NetworkSequenceViewReader reader) {
		availableReaders.add(reader);
		reader.setRegisteredAsAvailable(true);
	}
	
//writeListener类
private class WriteAndFlushNextMessageIfPossibleListener implements ChannelFutureListener {

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			try {
				if (future.isSuccess()) {
				//发送成功，再次尝试写入
					writeAndFlushNextMessageIfPossible(future.channel());
				} else if (future.cause() != null) {
					handleException(future.channel(), future.cause());
				} else {
					handleException(future.channel(), new IllegalStateException("Sending cancelled by user."));
				}
			} catch (Throwable t) {
				handleException(future.channel(), t);
			}
		}
	}
```

在 Credit-based Flow Control 算法中，每发送一个 buffer 就会消耗一点 credit，在消费端有空闲 buffer 可用时会发送 `AddCrdit` 消息。在前面的PartitionRequestServerHandler类中

```
class PartitionRequestServerHandler extends SimpleChannelInboundHandler<NettyMessage> {
    protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
		...
		 else if (msgClazz == AddCredit.class) {
		 //增加 credit
			AddCredit request = (AddCredit) msg;
			outboundQueue.addCredit(request.receiverId, request.credit);
       ...
}
```

PartitionRequestQueue类的addCredit

```
void addCredit(InputChannelID receiverId, int credit) throws Exception {
   if (fatalError) {
      return;
   }

   NetworkSequenceViewReader reader = allReaders.get(receiverId);
   if (reader != null) {
   //增加 credit
      reader.addCredit(credit);
//因为增加了credit，可能可以继续处理数据，因此把 reader 加入队列
      enqueueAvailableReader(reader);
   } else {
      throw new IllegalStateException("No reader for receiverId = " + receiverId + " exists.");
   }
}
```

![](bpSourceCodeReceiving.png)

而消费端即 `NettyClient`的入口为CreditBasedPartitionRequestClientHandler。

```
class CreditBasedPartitionRequestClientHandler extends ChannelInboundHandlerAdapter implements NetworkClientHandler {

   private static final Logger LOG = LoggerFactory.getLogger(CreditBasedPartitionRequestClientHandler.class);

   /** Channels, which already requested partitions from the producers. */
   private final ConcurrentMap<InputChannelID, RemoteInputChannel> inputChannels = new ConcurrentHashMap<>();

   /** Channels, which will notify the producers about unannounced credit. */
   private final ArrayDeque<RemoteInputChannel> inputChannelsWithCredit = new ArrayDeque<>();

   private final AtomicReference<Throwable> channelError = new AtomicReference<>();

   private final ChannelFutureListener writeListener = new WriteAndFlushNextMessageIfPossibleListener();
   
   @Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
	//从netty channel中接收到数据
		try {
		//解析消息
			decodeMsg(msg);
		} catch (Throwable t) {
			notifyAllChannelsOfErrorAndClose(t);
		}
	}
	
	private void decodeMsg(Object msg) throws Throwable {
		final Class<?> msgClazz = msg.getClass();

		// ---- Buffer --------------------------------------------------------
		if (msgClazz == NettyMessage.BufferResponse.class) {
		//正常的数据
			NettyMessage.BufferResponse bufferOrEvent = (NettyMessage.BufferResponse) msg;

//根据 ID 定位到对应的 RemoteInputChannel
			RemoteInputChannel inputChannel = inputChannels.get(bufferOrEvent.receiverId);
			if (inputChannel == null) {
			//如果没有对应的 RemoteInputChannel
				bufferOrEvent.releaseBuffer();
//取消对给定 receiverId 的订阅
				cancelRequestFor(bufferOrEvent.receiverId);

				return;
			}
//解析消息，是buffer还是event
			decodeBufferOrEvent(inputChannel, bufferOrEvent);

		} 
		......
	}
	
	
	private void decodeBufferOrEvent(RemoteInputChannel inputChannel, NettyMessage.BufferResponse bufferOrEvent) throws Throwable {
		try {
			ByteBuf nettyBuffer = bufferOrEvent.getNettyBuffer();
			final int receivedSize = nettyBuffer.readableBytes();
			if (bufferOrEvent.isBuffer()) {
				// ---- Buffer ------------------------------------------------

				// Early return for empty buffers. Otherwise Netty's readBytes() throws an
				// IndexOutOfBoundsException.
				if (receivedSize == 0) {
					inputChannel.onEmptyBuffer(bufferOrEvent.sequenceNumber, bufferOrEvent.backlog);
					return;
				}
            //从对应的 RemoteInputChannel 中请求一个 Buffer
				Buffer buffer = inputChannel.requestBuffer();
				if (buffer != null) {
				//将接收的数据写入buffer
					nettyBuffer.readBytes(buffer.asByteBuf(), receivedSize);
					buffer.setCompressed(bufferOrEvent.isCompressed);
//通知对应的channel，backlog是生产者那边堆积的buffer数量
					inputChannel.onBuffer(buffer, bufferOrEvent.sequenceNumber, bufferOrEvent.backlog);
				} else if (inputChannel.isReleased()) {
					cancelRequestFor(bufferOrEvent.receiverId);
				} else {
					throw new IllegalStateException("No buffer available in credit-based input channel.");
				}
			} else {
				// ---- Event -------------------------------------------------
				// TODO We can just keep the serialized data in the Netty buffer and release it later at the reader
				byte[] byteArray = new byte[receivedSize];
				nettyBuffer.readBytes(byteArray);

				MemorySegment memSeg = MemorySegmentFactory.wrap(byteArray);
				//是一个事件，不需要从 RemoteInputChannel 中申请 buffer
				Buffer buffer = new NetworkBuffer(memSeg, FreeingBufferRecycler.INSTANCE, false, receivedSize);
//通知对应的channel，backlog是生产者那边堆积的buffer数量
				inputChannel.onBuffer(buffer, bufferOrEvent.sequenceNumber, bufferOrEvent.backlog);
			}
		} finally {
			bufferOrEvent.releaseBuffer();
		}
	}
```

`CreditBasedPartitionRequestClientHandler` 从网络中读取数据后交给 `RemoteInputChannel`， `RemoteInputChannel` 会将接收到的加入队列中，并根据生产端的堆积申请 floating buffer：

RemoteInputChannel类onBuffer:

```
//接收到远程 ResultSubpartition 发送的 Buffer
public void onBuffer(Buffer buffer, int sequenceNumber, int backlog) throws IOException {
   boolean recycleBuffer = true;

   try {

      final boolean wasEmpty;
      synchronized (receivedBuffers) {
         // Similar to notifyBufferAvailable(), make sure that we never add a buffer
         // after releaseAllResources() released all buffers from receivedBuffers
         // (see above for details).
         if (isReleased.get()) {
            return;
         }

//序号需要匹配
         if (expectedSequenceNumber != sequenceNumber) {
            onError(new BufferReorderingException(expectedSequenceNumber, sequenceNumber));
            return;
         }
//加入 receivedBuffers 队列中
         wasEmpty = receivedBuffers.isEmpty();
         receivedBuffers.add(buffer);
         recycleBuffer = false;
      }

      ++expectedSequenceNumber;

      if (wasEmpty) {
      //通知 InputGate，当前 channel 有新数据
         notifyChannelNonEmpty();
      }

      if (backlog >= 0) {
      //根据客户端的积压申请float buffer
         onSenderBacklog(backlog);
      }
   } finally {
      if (recycleBuffer) {
         buffer.recycleBuffer();
      }
   }
}

//backlog 是发送端的堆积 的 buffer 数量，
	//如果 bufferQueue 中 buffer 的数量不足，就去须从 LocalBufferPool 中请求 floating buffer
	//在请求了新的 buffer 后，通知生产者有 credit 可用
void onSenderBacklog(int backlog) throws IOException {
		int numRequestedBuffers = 0;

		synchronized (bufferQueue) {
			// Similar to notifyBufferAvailable(), make sure that we never add a buffer
			// after releaseAllResources() released all buffers (see above for details).
			if (isReleased.get()) {
				return;
			}

//需要的 buffer 数量是 backlog + initialCredit, backlog 是生产者当前的积压
			numRequiredBuffers = backlog + initialCredit;
			while (bufferQueue.getAvailableBufferSize() < numRequiredBuffers && !isWaitingForFloatingBuffers) {
			//不停地请求新的 floating buffer
				Buffer buffer = inputGate.getBufferPool().requestBuffer();
				if (buffer != null) {
				//从 buffer poll 中请求到 buffer
					bufferQueue.addFloatingBuffer(buffer);
					numRequestedBuffers++;
				} else if (inputGate.getBufferProvider().addBufferListener(this)) {
				// buffer pool 没有 buffer 了，加一个监听，当 LocalBufferPool 中有新的 buffer 时会回调 notifyBufferAvailable
					// If the channel has not got enough buffers, register it as listener to wait for more floating buffers.
					isWaitingForFloatingBuffers = true;
					break;
				}
			}
		}
		
		if (numRequestedBuffers > 0 && unannouncedCredit.getAndAdd(numRequestedBuffers) == 0) {
		//请求了新的floating buffer，要更新 credit
			notifyCreditAvailable();
		}
	}
	

 private void notifyCreditAvailable() {
		checkState(partitionRequestClient != null, "Tried to send task event to producer before requesting a queue.");
//通知当前 channel 有新的 credit
		partitionRequestClient.notifyCreditAvailable(this);
	}
	
	//LocalBufferPool 的recycle回收buffer并通知有 buffer 可用
@Override
	public NotificationResult notifyBufferAvailable(Buffer buffer) {
		NotificationResult notificationResult = NotificationResult.BUFFER_NOT_USED;
		try {
			synchronized (bufferQueue) {
				checkState(isWaitingForFloatingBuffers,
					"This channel should be waiting for floating buffers.");

				// Important: make sure that we never add a buffer after releaseAllResources()
				// released all buffers. Following scenarios exist:
				// 1) releaseAllResources() already released buffers inside bufferQueue
				// -> then isReleased is set correctly
				// 2) releaseAllResources() did not yet release buffers from bufferQueue
				// -> we may or may not have set isReleased yet but will always wait for the
				// lock on bufferQueue to release buffers
				if (isReleased.get() || bufferQueue.getAvailableBufferSize() >= numRequiredBuffers) {
					isWaitingForFloatingBuffers = false;
					return notificationResult;
				}

//增加floating buffer
				bufferQueue.addFloatingBuffer(buffer);

				if (bufferQueue.getAvailableBufferSize() == numRequiredBuffers) {
				//bufferQueue中有足够多的 buffer 了
					isWaitingForFloatingBuffers = false;
					notificationResult = NotificationResult.BUFFER_USED_NO_NEED_MORE;
				} else {
				//bufferQueue 中 buffer 仍然不足
					notificationResult = NotificationResult.BUFFER_USED_NEED_MORE;
				}
			}

			if (unannouncedCredit.getAndAdd(1) == 0) {
				notifyCreditAvailable();
			}
		} catch (Throwable t) {
			setError(t);
		}
		return notificationResult;
	}
```

一旦 `RemoteInputChannel` 申请到新的 buffer，就需要通知生产者更新 credit，这需要发送一条 `AddCredit`消息：

NettyPartitionRequestClient.notifyCreditAvailable

```
public void notifyCreditAvailable(RemoteInputChannel inputChannel) {
   clientHandler.notifyCreditAvailable(inputChannel);
}
```

CreditBasedPartitionRequestClientHandler.notifyCreditAvailable

```
public void notifyCreditAvailable(final RemoteInputChannel inputChannel) {
//有新的credit触发一次自定义事件
   ctx.executor().execute(() -> ctx.pipeline().fireUserEventTriggered(inputChannel));
}

//触发自定义事件
public void userEventTriggered(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg instanceof RemoteInputChannel) {
		//有新的credit会触发
			boolean triggerWrite = inputChannelsWithCredit.isEmpty();
         //加入到队列中
			inputChannelsWithCredit.add((RemoteInputChannel) msg);

			if (triggerWrite) {
				writeAndFlushNextMessageIfPossible(ctx.channel());
			}
		} else {
			ctx.fireUserEventTriggered(msg);
		}
	}
	
	
private void writeAndFlushNextMessageIfPossible(Channel channel) {
		if (channelError.get() != null || !channel.isWritable()) {
			return;
		}
//从队列中取出 RemoteInputChannel， 发送消息
		while (true) {
			RemoteInputChannel inputChannel = inputChannelsWithCredit.poll();

			// The input channel may be null because of the write callbacks
			// that are executed after each write.
			if (inputChannel == null) {
				return;
			}

			//It is no need to notify credit for the released channel.
			if (!inputChannel.isReleased()) {
				AddCredit msg = new AddCredit(
				//发送 AddCredit 的消息
					inputChannel.getPartitionId(),
					inputChannel.getAndResetUnannouncedCredit(),
					inputChannel.getInputChannelId());

				// Write and flush and wait until this is done before
				// trying to continue with the next input channel.
				//发送成功之后通过writeListener监听继续发送
				channel.writeAndFlush(msg).addListener(writeListener);

				return;
			}
		}
	}
	
	
private class WriteAndFlushNextMessageIfPossibleListener implements ChannelFutureListener {

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			try {
				if (future.isSuccess()) {
					writeAndFlushNextMessageIfPossible(future.channel());
				} else if (future.cause() != null) {
					notifyAllChannelsOfErrorAndClose(future.cause());
				} else {
					notifyAllChannelsOfErrorAndClose(new IllegalStateException("Sending cancelled by user."));
				}
			} catch (Throwable t) {
				notifyAllChannelsOfErrorAndClose(t);
			}
		}
	}
}
```

Credit-based Flow Control 的具体机制为：

- 接收端向发送端声明可用的 Credit（一个可用的 buffer 对应一点 credit）；
- 当发送端获得了 X 点 Credit，表明它可以向网络中发送 X 个 buffer；当接收端分配了 X 点 Credit 给发送端，表明它有 X 个空闲的 buffer 可以接收数据；
- 只有在 Credit > 0 的情况下发送端才发送 buffer；发送端每发送一个 buffer，Credit 也相应地减少一点
- 由于 `CheckpointBarrier`，`EndOfPartitionEvent` 等事件可以被立即处理，因而事件可以立即发送，无需使用 Credit
- 当发送端发送 buffer 的时候，它同样把当前堆积的 buffer 数量（backlog size）告知接收端；接收端根据发送端堆积的数量来申请 floating buffer

这种流量控制机制可以有效地改善网络的利用率，不会因为 buffer 长时间停留在网络链路中进而导致整个所有的 Task 都无法继续处理数据，也无法进行 Checkpoint 操作。但是它的一个潜在的缺点是增加了上下游之间的通信成本（需要发送 credit 和 backlog 信息）



在上面几节，我们已经详细地分析了 Task 之间的数据交换机制和它们的实现原理，理解这这些实际上就已经理解了 Flink 的“反压”机制。

所谓“反压”，就是指在流处理系统中，下游任务的处理速度跟不上上游任务的数据生产速度。许多日常问题都会导致反压，例如，垃圾回收停顿可能会导致流入的数据快速堆积，或者遇到大促或秒杀活动导致流量陡增。反压如果不能得到正确的处理，可能会导致资源耗尽甚至系统崩溃。反压机制就是指系统能够自己检测到被阻塞的算子，然后系统自适应地降低源头或者上游的发送速率。在 Flink 中，应对“反压”是一种极其自然的方式，因为 Flink 中的数据传输机制已经提供了应对反压的措施。

在本地数据交换的情况下，两个 Task 实际上是同一个 JVM 中的两个线程，Task1 产生的 Buffer 直接被 Task2 使用，当 Task2 处理完之后这个 Buffer 就会被回收到本地缓冲池中。一旦 Task2 的处理速度比 Task2 产生 Buffer 的速度慢，那么缓冲池中 Buffer 渐渐地就会被耗尽，Task1 无法申请到新的 Buffer 自然就会阻塞，因而会导致 Task1 的降速。

在网络数据交换的情况下，如果下游 Task 的处理速度较慢，下游 Task 的接收缓冲池逐渐耗尽后就无法从网络中读取新的数据，这回导致上游 Task 无法将缓冲池中的 Buffer 发送到网络中，因此上游 Task 的缓冲池也会被耗尽，进而导致上游任务的降速。为了解决网络连接阻塞导致所有 Task 都无法处理数据的情况，Flink 还引入了 Credit-based Flow Control 算法，在上游生产者下游消费只之间通过“信用点”来协调发送速度，确保网络连接永远不会被阻塞。同时，Flink 的网络栈基于 Netty 构建，通过 Netty 的水位线机制也可以控制发送端的发送速率。