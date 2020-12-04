streamGraph结构如下：

![](2.png)



转化过程如下：

![](3.png)

```
env.execute()
```



execute是flink流执行的入口，进入StreamExecutionEnvironment类开始执行DAG：

StreamExecutionEnvironment类：

```
public JobExecutionResult execute(String jobName) throws Exception {
	Preconditions.checkNotNull(jobName, "Streaming Job name should not be null.");

	return execute(getStreamGraph(jobName));
}
```

调用StreamExecutionEnvironment类getStreamGraph得到streamGraph

```
public StreamGraph getStreamGraph(String jobName, boolean clearTransformations) {
   StreamGraph streamGraph = getStreamGraphGenerator().setJobName(jobName).generate();
   if (clearTransformations) {
      this.transformations.clear();
   }
   return streamGraph;
}
```

调用getStreamGraphGenerator方法初始化StreamGraphGenerator类，StreamGraphGenerator用于把Transformation转化为StreamGraph。

```
private StreamGraphGenerator getStreamGraphGenerator() {
   if (transformations.size() <= 0) {
      throw new IllegalStateException("No operators defined in streaming topology. Cannot execute.");
   }
   return new StreamGraphGenerator(transformations, config, checkpointCfg)
      .setStateBackend(defaultStateBackend)
      .setChaining(isChainingEnabled)  //是否允许chain
      .setUserArtifacts(cacheFile)
      .setTimeCharacteristic(timeCharacteristic) //时间类型，event或process
      .setDefaultBufferTimeout(bufferTimeout);
}
```

初始化之后调用generate方法开启转化过程：

```
public StreamGraph generate() {
   streamGraph = new StreamGraph(executionConfig, checkpointConfig, savepointRestoreSettings);
   streamGraph.setStateBackend(stateBackend);
   streamGraph.setChaining(chaining);
   streamGraph.setScheduleMode(scheduleMode);
   streamGraph.setUserArtifacts(userArtifacts);
   streamGraph.setTimeCharacteristic(timeCharacteristic);
   streamGraph.setJobName(jobName);
   streamGraph.setBlockingConnectionsBetweenChains(blockingConnectionsBetweenChains);

   alreadyTransformed = new HashMap<>();

  //遍历算子并转化
   for (Transformation<?> transformation: transformations) {
      transform(transformation);
   }

   final StreamGraph builtStreamGraph = streamGraph;

   alreadyTransformed.clear();
   alreadyTransformed = null;
   streamGraph = null;

   return builtStreamGraph;
}
```

StreamGraph类是StreamGraph的数据结构，用于构造jobGraph

```
public class StreamGraph implements Pipeline {
   ......
   private Map<Integer, StreamNode> streamNodes;
   private Set<Integer> sources;
   private Set<Integer> sinks;
   private Map<Integer, Tuple2<Integer, List<String>>> virtualSelectNodes;
   private Map<Integer, Tuple2<Integer, OutputTag>> virtualSideOutputNodes;
   private Map<Integer, Tuple3<Integer, StreamPartitioner<?>, ShuffleMode>> virtualPartitionNodes;

   protected Map<Integer, String> vertexIDtoBrokerID;
   protected Map<Integer, Long> vertexIDtoLoopTimeout;
   private StateBackend stateBackend;
   private Set<Tuple2<StreamNode, StreamNode>> iterationSourceSinkPairs;

   public StreamGraph(ExecutionConfig executionConfig, CheckpointConfig checkpointConfig, SavepointRestoreSettings savepointRestoreSettings) {
      this.executionConfig = checkNotNull(executionConfig);
      this.checkpointConfig = checkNotNull(checkpointConfig);
      this.savepointRestoreSettings = checkNotNull(savepointRestoreSettings);

      // create an empty new stream graph.
      clear();
   }
```

generate方法中，对所有Transformation调用transform方法。transform里面的主要逻辑是根据不同的Transformation类型执行不同的transform方法，就是大量if/else的部分。

Transformation大致有以下几种类型，其中有一些transform只是逻辑概念，比如 union、split/select、partition等，在转化的时候只会生成虚拟节点，也就是没有node，只在添加edge的时候进行特殊标注。

![](0.png)

```
private Collection<Integer> transform(Transformation<?> transform) {

   if (alreadyTransformed.containsKey(transform)) {
      return alreadyTransformed.get(transform);
   }

   LOG.debug("Transforming " + transform);

   if (transform.getMaxParallelism() <= 0) {

      // if the max parallelism hasn't been set, then first use the job wide max parallelism
      // from the ExecutionConfig.
      int globalMaxParallelismFromConfig = executionConfig.getMaxParallelism();
      if (globalMaxParallelismFromConfig > 0) {
         transform.setMaxParallelism(globalMaxParallelismFromConfig);
      }
   }

   // call at least once to trigger exceptions about MissingTypeInfo
   transform.getOutputType();

   Collection<Integer> transformedIds;
   if (transform instanceof OneInputTransformation<?, ?>) {
      transformedIds = transformOneInputTransform((OneInputTransformation<?, ?>) transform);
   } else if (transform instanceof TwoInputTransformation<?, ?, ?>) {
      transformedIds = transformTwoInputTransform((TwoInputTransformation<?, ?, ?>) transform);
   } else if (transform instanceof SourceTransformation<?>) {
      transformedIds = transformSource((SourceTransformation<?>) transform);
   } else if (transform instanceof SinkTransformation<?>) {
      transformedIds = transformSink((SinkTransformation<?>) transform);
   } else if (transform instanceof UnionTransformation<?>) {
      transformedIds = transformUnion((UnionTransformation<?>) transform);
   } else if (transform instanceof SplitTransformation<?>) {
      transformedIds = transformSplit((SplitTransformation<?>) transform);
   } else if (transform instanceof SelectTransformation<?>) {
      transformedIds = transformSelect((SelectTransformation<?>) transform);
   } else if (transform instanceof FeedbackTransformation<?>) {
      transformedIds = transformFeedback((FeedbackTransformation<?>) transform);
   } else if (transform instanceof CoFeedbackTransformation<?>) {
      transformedIds = transformCoFeedback((CoFeedbackTransformation<?>) transform);
   } else if (transform instanceof PartitionTransformation<?>) {
      transformedIds = transformPartition((PartitionTransformation<?>) transform);
   } else if (transform instanceof SideOutputTransformation<?>) {
      transformedIds = transformSideOutput((SideOutputTransformation<?>) transform);
   } else {
      throw new IllegalStateException("Unknown transformation: " + transform);
   }

   // need this check because the iterate transformation adds itself before
   // transforming the feedback edges
   if (!alreadyTransformed.containsKey(transform)) {
      alreadyTransformed.put(transform, transformedIds);
   }

   if (transform.getBufferTimeout() >= 0) {
      streamGraph.setBufferTimeout(transform.getId(), transform.getBufferTimeout());
   } else {
      streamGraph.setBufferTimeout(transform.getId(), defaultBufferTimeout);
   }

   if (transform.getUid() != null) {
      streamGraph.setTransformationUID(transform.getId(), transform.getUid());
   }
   if (transform.getUserProvidedNodeHash() != null) {
      streamGraph.setTransformationUserHash(transform.getId(), transform.getUserProvidedNodeHash());
   }

   if (!streamGraph.getExecutionConfig().hasAutoGeneratedUIDsEnabled()) {
      if (transform instanceof PhysicalTransformation &&
            transform.getUserProvidedNodeHash() == null &&
            transform.getUid() == null) {
         throw new IllegalStateException("Auto generated UIDs have been disabled " +
            "but no UID or hash has been assigned to operator " + transform.getName());
      }
   }

   if (transform.getMinResources() != null && transform.getPreferredResources() != null) {
      streamGraph.setResources(transform.getId(), transform.getMinResources(), transform.getPreferredResources());
   }

   streamGraph.setManagedMemoryWeight(transform.getId(), transform.getManagedMemoryWeight());

   return transformedIds;
}
```

看下几个典型算子的操作source,sink和transformOneInputTransform：

```
private <T> Collection<Integer> transformSource(SourceTransformation<T> source) {
   String slotSharingGroup = determineSlotSharingGroup(source.getSlotSharingGroup(), Collections.emptyList());

   streamGraph.addSource(source.getId(),
         slotSharingGroup,
         source.getCoLocationGroupKey(),
         source.getOperatorFactory(),
         null,
         source.getOutputType(),
         "Source: " + source.getName());
   if (source.getOperatorFactory() instanceof InputFormatOperatorFactory) {
      streamGraph.setInputFormat(source.getId(),
            ((InputFormatOperatorFactory<T>) source.getOperatorFactory()).getInputFormat());
   }
   int parallelism = source.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT ?
      source.getParallelism() : executionConfig.getParallelism();
   streamGraph.setParallelism(source.getId(), parallelism);
   streamGraph.setMaxParallelism(source.getId(), source.getMaxParallelism());
   return Collections.singleton(source.getId());
}
```

determineSlotSharingGroup方法确定算子的分组信息，只有同一分组内的算子可以chain在一起。分组可以由用户指定，或者使用默认分组"default"。

```
private String determineSlotSharingGroup(String specifiedGroup, Collection<Integer> inputIds) {
   if (specifiedGroup != null) {
      return specifiedGroup;
   } else {
      String inputGroup = null;
      for (int id: inputIds) {
         String inputGroupCandidate = streamGraph.getSlotSharingGroup(id);
         if (inputGroup == null) {
           //如果所有入流的group id都相同，则使用该group id
            inputGroup = inputGroupCandidate;
         } else if (!inputGroup.equals(inputGroupCandidate)) {
           //如果有不一样的id存在，则使用默认id default
            return DEFAULT_SLOT_SHARING_GROUP;
         }
      }
      return inputGroup == null ? DEFAULT_SLOT_SHARING_GROUP : inputGroup;
   }
}
```

StreamGraph类的addSource添加source算子，vertexID是一个从0开始全局递增的数字。

```
public <IN, OUT> void addSource(Integer vertexID,
   @Nullable String slotSharingGroup,
   @Nullable String coLocationGroup,
   StreamOperatorFactory<OUT> operatorFactory,
   TypeInformation<IN> inTypeInfo,
   TypeInformation<OUT> outTypeInfo,
   String operatorName) {
   addOperator(vertexID, slotSharingGroup, coLocationGroup, operatorFactory, inTypeInfo, outTypeInfo, operatorName);
   sources.add(vertexID);
}
```

addSource里面调用addOperator添加transform中的map算子，operator是transform的成员类，封装了用户定义的函数，它们的关系如下图所示。

![](1.png)

```
public <IN, OUT> void addOperator(
      Integer vertexID,
      @Nullable String slotSharingGroup,
      @Nullable String coLocationGroup,
      StreamOperatorFactory<OUT> operatorFactory,
      TypeInformation<IN> inTypeInfo,
      TypeInformation<OUT> outTypeInfo,
      String operatorName) {

//区分source和OneInputStreamTask
   if (operatorFactory.isStreamSource()) {
      addNode(vertexID, slotSharingGroup, coLocationGroup, SourceStreamTask.class, operatorFactory, operatorName);
   } else {
      addNode(vertexID, slotSharingGroup, coLocationGroup, OneInputStreamTask.class, operatorFactory, operatorName);
   }

   TypeSerializer<IN> inSerializer = inTypeInfo != null && !(inTypeInfo instanceof MissingTypeInfo) ? inTypeInfo.createSerializer(executionConfig) : null;

   TypeSerializer<OUT> outSerializer = outTypeInfo != null && !(outTypeInfo instanceof MissingTypeInfo) ? outTypeInfo.createSerializer(executionConfig) : null;

//为vertex设置入流和出流的序列化方式
   setSerializers(vertexID, inSerializer, null, outSerializer);

   if (operatorFactory.isOutputTypeConfigurable() && outTypeInfo != null) {
      // sets the output type which must be know at StreamGraph creation time
      operatorFactory.setOutputType(outTypeInfo, executionConfig);
   }

   if (operatorFactory.isInputTypeConfigurable()) {
      operatorFactory.setInputType(inTypeInfo, executionConfig);
   }

   if (LOG.isDebugEnabled()) {
      LOG.debug("Vertex: {}", vertexID);
   }
}
```

Operator算子最终转化成StreamGraph的vertex node，node中引入了变量 jobVertexClass 来表示该节点在 TaskManager 中运行时的实际任务类型。`AbstractInvokable` 是所有可以在 TaskManager 中运行的任务的抽象基础类，包括流式任务和批任务。`StreamTask` 是所有流式任务的基础类，其具体的子类包括 `SourceStreamTask`, `OneInputStreamTask`, `TwoInputStreamTask` 等。

```
protected StreamNode addNode(Integer vertexID,
   @Nullable String slotSharingGroup,
   @Nullable String coLocationGroup,
   Class<? extends AbstractInvokable> vertexClass,
   StreamOperatorFactory<?> operatorFactory,
   String operatorName) {

   if (streamNodes.containsKey(vertexID)) {
      throw new RuntimeException("Duplicate vertexID " + vertexID);
   }

   StreamNode vertex = new StreamNode(
      vertexID,
      slotSharingGroup,
      coLocationGroup,
      operatorFactory,
      operatorName,
      new ArrayList<OutputSelector<?>>(),
      vertexClass);

   streamNodes.put(vertexID, vertex);

   return vertex;
}
```

node是StreamNode类

```
public StreamNode(
   Integer id,
   @Nullable String slotSharingGroup,
   @Nullable String coLocationGroup,
   StreamOperatorFactory<?> operatorFactory,
   String operatorName,
   List<OutputSelector<?>> outputSelector,
   Class<? extends AbstractInvokable> jobVertexClass) {

   this.id = id;
   this.operatorName = operatorName;
   this.operatorFactory = operatorFactory;
   this.outputSelectors = outputSelector;
   this.jobVertexClass = jobVertexClass;
   this.slotSharingGroup = slotSharingGroup;
   this.coLocationGroup = coLocationGroup;
}
```

transformOneInputTransform直接调用streamGraph.addOperator添加Operator算子及node，并且与transformSource不同的是要设置边edge，因为有入流

```
private <IN, OUT> Collection<Integer> transformOneInputTransform(OneInputTransformation<IN, OUT> transform) {

//对输入流进行transform，有重复的直接从alreadyTransformed的map里get
   Collection<Integer> inputIds = transform(transform.getInput());

   // the recursive call might have already transformed this
   if (alreadyTransformed.containsKey(transform)) {
      return alreadyTransformed.get(transform);
   }

   String slotSharingGroup = determineSlotSharingGroup(transform.getSlotSharingGroup(), inputIds);

   streamGraph.addOperator(transform.getId(),
         slotSharingGroup,
         transform.getCoLocationGroupKey(),
         transform.getOperatorFactory(),
         transform.getInputType(),
         transform.getOutputType(),
         transform.getName());

//对于keyedStream，我们还要记录它的keySelector方法
      //flink并不真正为每个keyedStream保存一个key，而是每次需要用到key的时候都使用keySelector方法进行计算
        //因此，我们自定义的keySelector方法需要保证幂等性
        //到后面介绍keyGroup的时候我们还会再次提到这一点
   if (transform.getStateKeySelector() != null) {
      TypeSerializer<?> keySerializer = transform.getStateKeyType().createSerializer(executionConfig);
      streamGraph.setOneInputStateKey(transform.getId(), transform.getStateKeySelector(), keySerializer);
   }

   int parallelism = transform.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT ?
      transform.getParallelism() : executionConfig.getParallelism();
   streamGraph.setParallelism(transform.getId(), parallelism);
   streamGraph.setMaxParallelism(transform.getId(), transform.getMaxParallelism());

//为当前节点和它的依赖节点建立边
        //这里可以看到之前提到的select union partition等逻辑节点被合并入edge的过程
   for (Integer inputId: inputIds) {
      streamGraph.addEdge(inputId, transform.getId(), 0);
   }

   return Collections.singleton(transform.getId());
}
```

调用streamGraph的addEdge添加边，

```
public void addEdge(Integer upStreamVertexID, Integer downStreamVertexID, int typeNumber) {
   addEdgeInternal(upStreamVertexID,
         downStreamVertexID,
         typeNumber,
         null,
         new ArrayList<String>(),
         null,
         null);

}

private void addEdgeInternal(Integer upStreamVertexID,
      Integer downStreamVertexID,
      int typeNumber,
      StreamPartitioner<?> partitioner,
      List<String> outputNames,
      OutputTag outputTag,
      ShuffleMode shuffleMode) {

//如果输入边是侧输出节点，则把side的输入边作为本节点的输入边，并递归调用
   if (virtualSideOutputNodes.containsKey(upStreamVertexID)) {
      int virtualId = upStreamVertexID;
      upStreamVertexID = virtualSideOutputNodes.get(virtualId).f0;
      if (outputTag == null) {
         outputTag = virtualSideOutputNodes.get(virtualId).f1;
      }
      addEdgeInternal(upStreamVertexID, downStreamVertexID, typeNumber, partitioner, null, outputTag, shuffleMode);
   } else if (virtualSelectNodes.containsKey(upStreamVertexID)) {
   //如果输入边是select，则把select的输入边作为本节点的输入边
      int virtualId = upStreamVertexID;
      upStreamVertexID = virtualSelectNodes.get(virtualId).f0;
      if (outputNames.isEmpty()) {
         // selections that happen downstream override earlier selections
         outputNames = virtualSelectNodes.get(virtualId).f1;
      }
      addEdgeInternal(upStreamVertexID, downStreamVertexID, typeNumber, partitioner, outputNames, outputTag, shuffleMode);
   } else if (virtualPartitionNodes.containsKey(upStreamVertexID)) {
      int virtualId = upStreamVertexID;
      upStreamVertexID = virtualPartitionNodes.get(virtualId).f0;
      if (partitioner == null) {
         partitioner = virtualPartitionNodes.get(virtualId).f1;
      }
      shuffleMode = virtualPartitionNodes.get(virtualId).f2;
      addEdgeInternal(upStreamVertexID, downStreamVertexID, typeNumber, partitioner, outputNames, outputTag, shuffleMode);
   }
   //普通算子逻辑在这里
   else {
      StreamNode upstreamNode = getStreamNode(upStreamVertexID);
      StreamNode downstreamNode = getStreamNode(downStreamVertexID);

      // If no partitioner was specified and the parallelism of upstream and downstream
      // operator matches use forward partitioning, use rebalance otherwise.
      if (partitioner == null && upstreamNode.getParallelism() == downstreamNode.getParallelism()) {
         partitioner = new ForwardPartitioner<Object>();
      } else if (partitioner == null) {
         partitioner = new RebalancePartitioner<Object>();
      }

      if (partitioner instanceof ForwardPartitioner) {
         if (upstreamNode.getParallelism() != downstreamNode.getParallelism()) {
            throw new UnsupportedOperationException("Forward partitioning does not allow " +
                  "change of parallelism. Upstream operation: " + upstreamNode + " parallelism: " + upstreamNode.getParallelism() +
                  ", downstream operation: " + downstreamNode + " parallelism: " + downstreamNode.getParallelism() +
                  " You must use another partitioning strategy, such as broadcast, rebalance, shuffle or global.");
         }
      }

      if (shuffleMode == null) {
         shuffleMode = ShuffleMode.UNDEFINED;
      }

//新初始化StreamEdge的类
      StreamEdge edge = new StreamEdge(upstreamNode, downstreamNode, typeNumber, outputNames, partitioner, outputTag, shuffleMode);

//为node添加edge边，分OutEdge和InEdge
      getStreamNode(edge.getSourceId()).addOutEdge(edge);
      getStreamNode(edge.getTargetId()).addInEdge(edge);
   }
}
```

StreamEdge类

```
public StreamEdge(StreamNode sourceVertex, StreamNode targetVertex, int typeNumber,
      List<String> selectedNames, StreamPartitioner<?> outputPartitioner, OutputTag outputTag,
      ShuffleMode shuffleMode) {
   this.sourceId = sourceVertex.getId();
   this.targetId = targetVertex.getId();
   this.typeNumber = typeNumber;
   this.selectedNames = selectedNames;
   this.outputPartitioner = outputPartitioner;
   this.outputTag = outputTag;
   this.sourceOperatorName = sourceVertex.getOperatorName();
   this.targetOperatorName = targetVertex.getOperatorName();
   this.shuffleMode = checkNotNull(shuffleMode);

//edgeId是由多个属性字段拼接得到
   this.edgeId = sourceVertex + "_" + targetVertex + "_" + typeNumber + "_" + selectedNames
         + "_" + outputPartitioner;
}
```

Sink的转换逻辑和transformOneInputTransform类似，多了一个OutputFormat的设置以及streamGraph封装了一层addSink的逻辑

```
private <T> Collection<Integer> transformSink(SinkTransformation<T> sink) {

   Collection<Integer> inputIds = transform(sink.getInput());

   String slotSharingGroup = determineSlotSharingGroup(sink.getSlotSharingGroup(), inputIds);

   streamGraph.addSink(sink.getId(),
         slotSharingGroup,
         sink.getCoLocationGroupKey(),
         sink.getOperatorFactory(),
         sink.getInput().getOutputType(),
         null,
         "Sink: " + sink.getName());

   StreamOperatorFactory operatorFactory = sink.getOperatorFactory();
   if (operatorFactory instanceof OutputFormatOperatorFactory) {
      streamGraph.setOutputFormat(sink.getId(), ((OutputFormatOperatorFactory) operatorFactory).getOutputFormat());
   }

   int parallelism = sink.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT ?
      sink.getParallelism() : executionConfig.getParallelism();
   streamGraph.setParallelism(sink.getId(), parallelism);
   streamGraph.setMaxParallelism(sink.getId(), sink.getMaxParallelism());

   for (Integer inputId: inputIds) {
      streamGraph.addEdge(inputId,
            sink.getId(),
            0
      );
   }

   if (sink.getStateKeySelector() != null) {
      TypeSerializer<?> keySerializer = sink.getStateKeyType().createSerializer(executionConfig);
      streamGraph.setOneInputStateKey(sink.getId(), sink.getStateKeySelector(), keySerializer);
   }

   return Collections.emptyList();
}
```

addSink最终还是调用addOperator逻辑，并保存所有sink的id

```
public <IN, OUT> void addSink(Integer vertexID,
   @Nullable String slotSharingGroup,
   @Nullable String coLocationGroup,
   StreamOperatorFactory<OUT> operatorFactory,
   TypeInformation<IN> inTypeInfo,
   TypeInformation<OUT> outTypeInfo,
   String operatorName) {
   addOperator(vertexID, slotSharingGroup, coLocationGroup, operatorFactory, inTypeInfo, outTypeInfo, operatorName);
   sinks.add(vertexID);
}
```

其余transform类似，虚拟算子不生成node，只在添加edge的时候进行设置。