JobGraph的结构：

![](1.png)

StreamGraph生成的过程是StreamExecutionEnvironment类execute，通过getStreamGraph得到StreamGraph：

```
public JobExecutionResult execute(String jobName) throws Exception {
	Preconditions.checkNotNull(jobName, "Streaming Job name should not be null.");

	return execute(getStreamGraph(jobName));
}
```

继续执行execute，调用了异步方法executeAsync

```
public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
   final JobClient jobClient = executeAsync(streamGraph);
  ......
}
```

异步执行streamGraph调度，Executor根据环境有LocalExecutor(本地)，AbstractJobClusterExecutor(job模式)，AbstractSessionClusterExecutor(session模式)

```
public JobClient executeAsync(StreamGraph streamGraph) throws Exception {
   checkNotNull(streamGraph, "StreamGraph cannot be null.");
   checkNotNull(configuration.get(DeploymentOptions.TARGET), "No execution.target specified in your configuration file.");

   final PipelineExecutorFactory executorFactory =
      executorServiceLoader.getExecutorFactory(configuration);

   checkNotNull(
      executorFactory,
      "Cannot find compatible factory for specified execution.target (=%s)",
      configuration.get(DeploymentOptions.TARGET));

   CompletableFuture<JobClient> jobClientFuture = executorFactory
      .getExecutor(configuration)
      .execute(streamGraph, configuration);
      ......
  }
```

以LocalExecutor为例

```
public CompletableFuture<JobClient> execute(Pipeline pipeline, Configuration configuration) throws Exception {
   checkNotNull(pipeline);
   checkNotNull(configuration);

   // we only support attached execution with the local executor.
   checkState(configuration.getBoolean(DeploymentOptions.ATTACHED));

   final JobGraph jobGraph = getJobGraph(pipeline, configuration);
 ......
 }
```

调用getJobGraph

```
private JobGraph getJobGraph(Pipeline pipeline, Configuration configuration) {
   ......

   return FlinkPipelineTranslationUtil.getJobGraph(pipeline, configuration, 1);
}

FlinkPipelineTranslationUtil#getJobGraph：

public static JobGraph getJobGraph(
			Pipeline pipeline,
			Configuration optimizerConfiguration,
			int defaultParallelism) {

		FlinkPipelineTranslator pipelineTranslator = getPipelineTranslator(pipeline);

		return pipelineTranslator.translateToJobGraph(pipeline,
				optimizerConfiguration,
				defaultParallelism);
	}
```

FlinkPipelineTranslator根据API的不同分StreamGraphTranslator(流处理)和PlanTranslator(批处理)，进入流处理的Translator

```
public JobGraph translateToJobGraph(
      Pipeline pipeline,
      Configuration optimizerConfiguration,
      int defaultParallelism) {
   checkArgument(pipeline instanceof StreamGraph,
         "Given pipeline is not a DataStream StreamGraph.");

   StreamGraph streamGraph = (StreamGraph) pipeline;
   return streamGraph.getJobGraph(null);
}
```

调用了StreamGraph的getJobGraph,最终进入了StreamingJobGraphGenerator的createJobGraph

```
public JobGraph getJobGraph(@Nullable JobID jobID) {
   return StreamingJobGraphGenerator.createJobGraph(this, jobID);
}
```

createJobGraph是JobGraph生成的主要逻辑

```
private JobGraph createJobGraph() {
   preValidate();

   // make sure that all vertices start immediately
   jobGraph.setScheduleMode(streamGraph.getScheduleMode());

   // Generate deterministic hashes for the nodes in order to identify them across
   // submission iff they didn't change.
   //有两种Hasher，StreamGraphHasherV2和StreamGraphUserHashHasher，生成的Map为（key-> StreamNode的id，value->为StreamNode生成的hash值，用来做jobVertex的id
   Map<Integer, byte[]> hashes = defaultStreamGraphHasher.traverseStreamGraphAndGenerateHashes(streamGraph);

   // Generate legacy version hashes for backwards compatibility
   // 为了保持兼容性创建的hash
   List<Map<Integer, byte[]>> legacyHashes = new ArrayList<>(legacyStreamGraphHashers.size());
   for (StreamGraphHasher hasher : legacyStreamGraphHashers) {
      legacyHashes.add(hasher.traverseStreamGraphAndGenerateHashes(streamGraph));
   }

   Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes = new HashMap<>();

//这里的逻辑大致可以理解为，挨个遍历节点，如果该节点是一个chain的头节点，就生成一个JobVertex，如果不是头节点，就要把自身配置并入头节点，然后把头节点和自己的出边相连；对于不能chain的节点，当作只有头节点处理即可
   setChaining(hashes, legacyHashes, chainedOperatorHashes);

// 将每个JobVertex的输入边集合也序列化到该JobVertex的StreamConfig中
    // (出边集合已经在setChaining的时候写入了)
   setPhysicalEdges();

// 根据group name，为每个 JobVertex 指定所属的 SlotSharingGroup
		// 以及针对 Iteration的头尾设置  CoLocationGroup
   setSlotSharingAndCoLocation();

   setManagedMemoryFraction(
      Collections.unmodifiableMap(jobVertices),
      Collections.unmodifiableMap(vertexConfigs),
      Collections.unmodifiableMap(chainedConfigs),
      id -> streamGraph.getStreamNode(id).getMinResources(),
      id -> streamGraph.getStreamNode(id).getManagedMemoryWeight());

// 配置 checkpoint
   configureCheckpointing();

   
  //设置Savepoint的配置信息
jobGraph.setSavepointRestoreSettings(streamGraph.getSavepointRestoreSettings());

// 添加用户提供的自定义的文件信息
   JobGraphGenerator.addUserArtifactEntries(streamGraph.getUserArtifacts(), jobGraph);

   // set the ExecutionConfig last when it has been finalized
   // 将 StreamGraph 的 ExecutionConfig 序列化到 JobGraph 的配置中
   try {
      jobGraph.setExecutionConfig(streamGraph.getExecutionConfig());
   }
   catch (IOException e) {
      throw new IllegalConfigurationException("Could not serialize the ExecutionConfig." +
            "This indicates that non-serializable types (like custom serializers) were registered");
   }

   return jobGraph;
}
```

createJobGraph的主要步骤是setChaining，

- chain的逻辑如下图：

  ![](2.png)

```
private void setChaining(Map<Integer, byte[]> hashes, List<Map<Integer, byte[]>> legacyHashes, Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {
   for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
      createChain(sourceNodeId, sourceNodeId, hashes, legacyHashes, 0, chainedOperatorHashes);
   }
}

//构建 operator chain（可能包含一个或多个StreamNode），返回值是当前的这个 operator chain 实际的输出边（不包括内部的边）
	//如果 currentNodeId != startNodeId, 说明当前节点在  operator chain 的内部
private List<StreamEdge> createChain(
      Integer startNodeId,
      Integer currentNodeId,
      Map<Integer, byte[]> hashes,
      List<Map<Integer, byte[]>> legacyHashes,
      int chainIndex,
      Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {

   if (!builtVertices.contains(startNodeId)) {

//当前 operator chain 最终的输出边，不包括内部的边
      List<StreamEdge> transitiveOutEdges = new ArrayList<StreamEdge>();

      List<StreamEdge> chainableOutputs = new ArrayList<StreamEdge>();
      List<StreamEdge> nonChainableOutputs = new ArrayList<StreamEdge>();

      StreamNode currentNode = streamGraph.getStreamNode(currentNodeId);

//将当前节点的出边分为两组，即 chainable 和 nonChainable
      for (StreamEdge outEdge : currentNode.getOutEdges()) {
         if (isChainable(outEdge, streamGraph)) {
            chainableOutputs.add(outEdge);
         } else {
            nonChainableOutputs.add(outEdge);
         }
      }

//对于chainable的输出边，递归调用，找到最终的输出边并加入到输出列表中
      for (StreamEdge chainable : chainableOutputs) {
         transitiveOutEdges.addAll(
               createChain(startNodeId, chainable.getTargetId(), hashes, legacyHashes, chainIndex + 1, chainedOperatorHashes));
      }

//对于 nonChainable 的边
      for (StreamEdge nonChainable : nonChainableOutputs) {
      //先把这个边本身加入到当前节点的输出列表中
         transitiveOutEdges.add(nonChainable);
         //递归调用，以下游节点为起点创建新的operator chain
         createChain(nonChainable.getTargetId(), nonChainable.getTargetId(), hashes, legacyHashes, 0, chainedOperatorHashes);
      }

//用于保存一个operator chain所有 operator 的 hash 信息，每个startNodeId对应一个list，保存在输入参数chainedOperatorHashes中，list的内容为tuple（currentNodeId的hash，currentNodeId的legacyHash），一个currentNodeId可以有多个tuple
      List<Tuple2<byte[], byte[]>> operatorHashes =
         chainedOperatorHashes.computeIfAbsent(startNodeId, k -> new ArrayList<>());

      byte[] primaryHashBytes = hashes.get(currentNodeId);
      OperatorID currentOperatorId = new OperatorID(primaryHashBytes);

      for (Map<Integer, byte[]> legacyHash : legacyHashes) {
         operatorHashes.add(new Tuple2<>(primaryHashBytes, legacyHash.get(currentNodeId)));
      }

//当前节点的名称，资源要求等信息
      chainedNames.put(currentNodeId, createChainedName(currentNodeId, chainableOutputs));
      chainedMinResources.put(currentNodeId, createChainedMinResources(currentNodeId, chainableOutputs));
      chainedPreferredResources.put(currentNodeId, createChainedPreferredResources(currentNodeId, chainableOutputs));

//用来生成InputOutputFormatVertex
      if (currentNode.getInputFormat() != null) {
         getOrCreateFormatContainer(startNodeId).addInputFormat(currentOperatorId, currentNode.getInputFormat());
      }

      if (currentNode.getOutputFormat() != null) {
         getOrCreateFormatContainer(startNodeId).addOutputFormat(currentOperatorId, currentNode.getOutputFormat());
      }

//如果当前节点是起始节点, 则直接创建 JobVertex 并返回 StreamConfig, 否则先创建一个空的 StreamConfig，createJobVertex 函数就是根据 StreamNode 创建对应的 JobVertex
      StreamConfig config = currentNodeId.equals(startNodeId)
            ? createJobVertex(startNodeId, hashes, legacyHashes, chainedOperatorHashes)
            : new StreamConfig(new Configuration());

// 设置 JobVertex 的 StreamConfig, 基本上是序列化 StreamNode 中的配置到 StreamConfig 中.其中包括 序列化器, StreamOperator, Checkpoint 等相关配置
      setVertexConfig(currentNodeId, config, chainableOutputs, nonChainableOutputs);

      if (currentNodeId.equals(startNodeId)) {
// 如果是chain的起始节点。（不是chain中的节点，也会被标记成 chain start）
         config.setChainStart();
         config.setChainIndex(0);
         config.setOperatorName(streamGraph.getStreamNode(currentNodeId).getOperatorName());
         //把实际的输出边写入配置, 部署时会用到
         config.setOutEdgesInOrder(transitiveOutEdges);
         //operator chain 的头部 operator 的输出边，包括内部的边config.setOutEdges(streamGraph.getStreamNode(currentNodeId).getOutEdges());
         
// 将当前节点(headOfChain)与所有出边相连
         for (StreamEdge edge : transitiveOutEdges) {
         // 通过StreamEdge构建出JobEdge，创建IntermediateDataSet，用来将JobVertex和JobEdge相连
            connect(startNodeId, edge);
         }

         // 将operator chain中所有子节点的 StreamConfig 写入到 headOfChain 节点的 CHAINED_TASK_CONFIG 配置中config.setTransitiveChainedTaskConfigs(chainedConfigs.get(startNodeId));

      } else {
      //对于chain的内部node，直接把自己的配置记录进startNodeId的chainedConfigs中
         chainedConfigs.computeIfAbsent(startNodeId, k -> new HashMap<Integer, StreamConfig>());

         config.setChainIndex(chainIndex);
         StreamNode node = streamGraph.getStreamNode(currentNodeId);
         config.setOperatorName(node.getOperatorName());
         chainedConfigs.get(startNodeId).put(currentNodeId, config);
      }

//设置当前 operator 的 OperatorID
      config.setOperatorID(currentOperatorId);

      if (chainableOutputs.isEmpty()) {
         config.setChainEnd();
      }
      return transitiveOutEdges;

   } else {
      return new ArrayList<>();
   }
}
```

算子chain在一起的条件是：

- 上下游的并行度一致

- 下游节点的入度为1 （也就是说下游节点没有来自其他节点的输入）

- 上下游节点都在同一个 slot group 中（下面会解释 slot group）

- 下游节点的 chain 策略为 ALWAYS（可以与上下游链接，map、flatmap、filter等默认是ALWAYS）

- 上游节点的 chain 策略为 ALWAYS 或 HEAD（只能与下游链接，不能与上游链接，Source默认是HEAD）

- 两个节点间数据分区方式是 forward（参考理解数据流的分区）

- 用户没有禁用 chain

  这个过程在isChainable中进行判断：

  ```
  public static boolean isChainable(StreamEdge edge, StreamGraph streamGraph) {
     StreamNode upStreamVertex = streamGraph.getSourceVertex(edge);
     StreamNode downStreamVertex = streamGraph.getTargetVertex(edge);
  
     StreamOperatorFactory<?> headOperator = upStreamVertex.getOperatorFactory();
     StreamOperatorFactory<?> outOperator = downStreamVertex.getOperatorFactory();
  
     return downStreamVertex.getInEdges().size() == 1
           && outOperator != null
           && headOperator != null
           && upStreamVertex.isSameSlotSharingGroup(downStreamVertex)
           && outOperator.getChainingStrategy() == ChainingStrategy.ALWAYS
           && (headOperator.getChainingStrategy() == ChainingStrategy.HEAD ||
              headOperator.getChainingStrategy() == ChainingStrategy.ALWAYS)
           && (edge.getPartitioner() instanceof ForwardPartitioner)
           && edge.getShuffleMode() != ShuffleMode.BATCH
           && upStreamVertex.getParallelism() == downStreamVertex.getParallelism()
           && streamGraph.isChainingEnabled();
  }
  ```

createJobVertex生成JobVertex，相当于jobghaph的顶点

```
private StreamConfig createJobVertex(
      Integer streamNodeId,
      Map<Integer, byte[]> hashes,
      List<Map<Integer, byte[]>> legacyHashes,
      Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {

   JobVertex jobVertex;
   StreamNode streamNode = streamGraph.getStreamNode(streamNodeId);

   byte[] hash = hashes.get(streamNodeId);

   if (hash == null) {
      throw new IllegalStateException("Cannot find node hash. " +
            "Did you generate them before calling this method?");
   }

//JobVertexID就是StreamID的hash
   JobVertexID jobVertexId = new JobVertexID(hash);

   List<JobVertexID> legacyJobVertexIds = new ArrayList<>(legacyHashes.size());
   for (Map<Integer, byte[]> legacyHash : legacyHashes) {
      hash = legacyHash.get(streamNodeId);
      if (null != hash) {
         legacyJobVertexIds.add(new JobVertexID(hash));
      }
   }

//得到chain算子的hash和userDefinedHash
   List<Tuple2<byte[], byte[]>> chainedOperators = chainedOperatorHashes.get(streamNodeId);
   List<OperatorID> chainedOperatorVertexIds = new ArrayList<>();
   List<OperatorID> userDefinedChainedOperatorVertexIds = new ArrayList<>();
   if (chainedOperators != null) {
      for (Tuple2<byte[], byte[]> chainedOperator : chainedOperators) {
         chainedOperatorVertexIds.add(new OperatorID(chainedOperator.f0));
         userDefinedChainedOperatorVertexIds.add(chainedOperator.f1 != null ? new OperatorID(chainedOperator.f1) : null);
      }
   }

   if (chainedInputOutputFormats.containsKey(streamNodeId)) {
   //生成InputOutputFormatVertex
      jobVertex = new InputOutputFormatVertex(
            chainedNames.get(streamNodeId),
            jobVertexId,
            legacyJobVertexIds,
            chainedOperatorVertexIds,
            userDefinedChainedOperatorVertexIds);

      chainedInputOutputFormats
         .get(streamNodeId)
         .write(new TaskConfig(jobVertex.getConfiguration()));
   } else {
   //生成jobVertex
      jobVertex = new JobVertex(
            chainedNames.get(streamNodeId),
            jobVertexId,
            legacyJobVertexIds,
            chainedOperatorVertexIds,
            userDefinedChainedOperatorVertexIds);
   }

   jobVertex.setResources(chainedMinResources.get(streamNodeId), chainedPreferredResources.get(streamNodeId));

   jobVertex.setInvokableClass(streamNode.getJobVertexClass());

   int parallelism = streamNode.getParallelism();

   if (parallelism > 0) {
      jobVertex.setParallelism(parallelism);
   } else {
      parallelism = jobVertex.getParallelism();
   }

   jobVertex.setMaxParallelism(streamNode.getMaxParallelism());

   if (LOG.isDebugEnabled()) {
      LOG.debug("Parallelism set: {} for {}", parallelism, streamNodeId);
   }

   // TODO: inherit InputDependencyConstraint from the head operator
   jobVertex.setInputDependencyConstraint(streamGraph.getExecutionConfig().getDefaultInputDependencyConstraint());

   jobVertices.put(streamNodeId, jobVertex);
   builtVertices.add(streamNodeId);
   jobGraph.addVertex(jobVertex);

   return new StreamConfig(jobVertex.getConfiguration());
}
```

JobVertex类，每个 `JobVertex` 中包含一个或多个 Operators，其输入是 `JobEdge` 列表, 输出是 `IntermediateDataSet` 列表。

```
public class JobVertex implements java.io.Serializable {

   private static final String DEFAULT_NAME = "(unnamed vertex)";

   // --------------------------------------------------------------------------------------------
   // Members that define the structure / topology of the graph
   // --------------------------------------------------------------------------------------------

   /** The ID of the vertex. */
   private final JobVertexID id;

   /** The alternative IDs of the vertex. */
   private final ArrayList<JobVertexID> idAlternatives = new ArrayList<>();

   /** The IDs of all operators contained in this vertex. */
   private final ArrayList<OperatorID> operatorIDs = new ArrayList<>();

   /** The alternative IDs of all operators contained in this vertex. */
   private final ArrayList<OperatorID> operatorIdsAlternatives = new ArrayList<>();

   /** List of produced data sets, one per writer. */
   private final ArrayList<IntermediateDataSet> results = new ArrayList<>();

   /** List of edges with incoming data. One per Reader. */
   private final ArrayList<JobEdge> inputs = new ArrayList<>();
```

每一个 operator chain 都会为所有的实际输出边创建对应的 `JobEdge`，并和 `JobVertex` 连接

```
private void connect(Integer headOfChain, StreamEdge edge) {

   physicalEdgesInOrder.add(edge);

   Integer downStreamvertexID = edge.getTargetId();

//获取上下游JobVertex
   JobVertex headVertex = jobVertices.get(headOfChain);
   JobVertex downStreamVertex = jobVertices.get(downStreamvertexID);

   StreamConfig downStreamConfig = new StreamConfig(downStreamVertex.getConfiguration());
//下游节点增加一个输入
   downStreamConfig.setNumberOfInputs(downStreamConfig.getNumberOfInputs() + 1);

   StreamPartitioner<?> partitioner = edge.getPartitioner();

//根据StreamEdge的Shuffle模式得到ResultPartitionType
   ResultPartitionType resultPartitionType;
   switch (edge.getShuffleMode()) {
      case PIPELINED:
         resultPartitionType = ResultPartitionType.PIPELINED_BOUNDED;
         break;
      case BATCH:
         resultPartitionType = ResultPartitionType.BLOCKING;
         break;
      case UNDEFINED:
         resultPartitionType = streamGraph.isBlockingConnectionsBetweenChains() ?
               ResultPartitionType.BLOCKING : ResultPartitionType.PIPELINED_BOUNDED;
         break;
      default:
         throw new UnsupportedOperationException("Data exchange mode " +
            edge.getShuffleMode() + " is not supported yet.");
   }

//创建 JobEdge 和 IntermediateDataSet，根据StreamPartitioner类型决定在上游节点（生产者）的子任务和下游节点（消费者）之间的连接模式
   JobEdge jobEdge;
   if (partitioner instanceof ForwardPartitioner || partitioner instanceof RescalePartitioner) {
      jobEdge = downStreamVertex.connectNewDataSetAsInput(
         headVertex,
         DistributionPattern.POINTWISE,
         resultPartitionType);
   } else {
      jobEdge = downStreamVertex.connectNewDataSetAsInput(
            headVertex,
            DistributionPattern.ALL_TO_ALL,
            resultPartitionType);
   }
   // set strategy name so that web interface can show it.
   jobEdge.setShipStrategyName(partitioner.toString());

   if (LOG.isDebugEnabled()) {
      LOG.debug("CONNECTED: {} - {} -> {}", partitioner.getClass().getSimpleName(),
            headOfChain, downStreamvertexID);
   }
}
```

connectNewDataSetAsInput方法在JobVertex类里面，由下游节点调用生成。

```
public JobEdge connectNewDataSetAsInput(
      JobVertex input,
      DistributionPattern distPattern,
      ResultPartitionType partitionType) {

   IntermediateDataSet dataSet = input.createAndAddResultDataSet(partitionType);

//创建JobEdge
   JobEdge edge = new JobEdge(dataSet, this, distPattern);
   this.inputs.add(edge);
   dataSet.addConsumer(edge);
   return edge;
}
```

JobEdge类，在 `StramGraph` 中，`StreamNode` 之间是通过 `StreamEdge` 建立连接的。在 `JobEdge` 中，对应的是 `JobEdge` 。和 `StreamEdge` 中同时保留了源节点和目标节点 （sourceId 和 targetId）不同，在 `JobEdge` 中只有源节点的信息。由于 `JobVertex` 中保存了所有输入的 `JobEdge` 的信息，因而同样可以在两个节点之间建立连接。更确切地说，`JobEdge` 是和节点的输出结果相关联的。

```
public class JobEdge implements java.io.Serializable {
      
   /** The vertex connected to this edge. */
   private final JobVertex target;

   /** The distribution pattern that should be used for this job edge. */
   private final DistributionPattern distributionPattern;
   
   /** The data set at the source of the edge, may be null if the edge is not yet connected*/
   private IntermediateDataSet source;
   
   /** The id of the source intermediate data set */
   private IntermediateDataSetID sourceId;
   
   /** Optional name for the data shipping strategy (forward, partition hash, rebalance, ...),
    * to be displayed in the JSON plan */
   private String shipStrategyName;

   /** Optional name for the pre-processing operation (sort, combining sort, ...),
    * to be displayed in the JSON plan */
   private String preProcessingOperationName;

   /** Optional description of the caching inside an operator, to be displayed in the JSON plan */
   private String operatorLevelCachingDescription;
```

生成IntermediateDataSet

```
public IntermediateDataSet createAndAddResultDataSet(ResultPartitionType partitionType) {
   return createAndAddResultDataSet(new IntermediateDataSetID(), partitionType);
}

public IntermediateDataSet createAndAddResultDataSet(
      IntermediateDataSetID id,
      ResultPartitionType partitionType) {

   IntermediateDataSet result = new IntermediateDataSet(id, partitionType, this);
   this.results.add(result);
   return result;
}
```

IntermediateDataSet代表中间数据流，连接JobVertex和JobEdge

```
/**
 * An intermediate data set is the data set produced by an operator - either a
 * source or any intermediate operation.
 * 
 * Intermediate data sets may be read by other operators, materialized, or
 * discarded.
 */
public class IntermediateDataSet implements java.io.Serializable {
   
   private static final long serialVersionUID = 1L;

   
   private final IntermediateDataSetID id;       // the identifier
   
   private final JobVertex producer;        // the operation that produced this data set
   
   private final List<JobEdge> consumers = new ArrayList<JobEdge>();

   // The type of partition to use at runtime
   private final ResultPartitionType resultType;

   public IntermediateDataSet(IntermediateDataSetID id, ResultPartitionType resultType, JobVertex producer) {
      this.id = checkNotNull(id);
      this.producer = checkNotNull(producer);
      this.resultType = checkNotNull(resultType);
   }
```