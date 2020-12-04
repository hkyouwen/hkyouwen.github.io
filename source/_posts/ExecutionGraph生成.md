ExcutionGraph结构:

![](1.png)

ExcutionGraph是在服务端（JobManager处）生成的，回顾之前的流程，streamGraph生成之后进入异步流程executeAsync

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

还是以LocalExecutor为例，异步执行LocalExecutor的excute，在excute中生成JobGraph，并启动集群，有集群客户端clusterClient将JobGraph提交至集群

```
public CompletableFuture<JobClient> execute(Pipeline pipeline, Configuration configuration) throws Exception {
   checkNotNull(pipeline);
   checkNotNull(configuration);

   // we only support attached execution with the local executor.
   checkState(configuration.getBoolean(DeploymentOptions.ATTACHED));

   final JobGraph jobGraph = getJobGraph(pipeline, configuration);
   final MiniCluster miniCluster = startMiniCluster(jobGraph, configuration);
   final MiniClusterClient clusterClient = new MiniClusterClient(configuration, miniCluster);

   CompletableFuture<JobID> jobIdFuture = clusterClient.submitJob(jobGraph);

//等待submit的结果并关闭客户端
   jobIdFuture
         .thenCompose(clusterClient::requestJobResult)
         .thenAccept((jobResult) -> clusterClient.shutDownCluster());

   return jobIdFuture.thenApply(jobID ->
         new ClusterClientJobClientAdapter<>(() -> clusterClient, jobID));
}
```

由MiniClusterClient客户端提交至MiniCluster的submitJob，submitJob将jobGraph提交到flink集群的Dispatcher，由Dispatcher生成jobmanager

```
public CompletableFuture<JobSudispatcherbmissionResult> submitJob(JobGraph jobGraph) {
//获取dispatcher地址
   final CompletableFuture<DispatcherGateway> dispatcherGatewayFuture = getDispatcherGatewayFuture();
   final CompletableFuture<InetSocketAddress> blobServerAddressFuture = createBlobServerAddress(dispatcherGatewayFuture);
   //上传jar包至Dispatcher
   final CompletableFuture<Void> jarUploadFuture = uploadAndSetJobFiles(blobServerAddressFuture, jobGraph);
   final CompletableFuture<Acknowledge> acknowledgeCompletableFuture = jarUploadFuture
      .thenCombine(
         dispatcherGatewayFuture,
         //向dispatcher提交jobGraph
         (Void ack, DispatcherGateway dispatcherGateway) -> dispatcherGateway.submitJob(jobGraph, rpcTimeout))
      .thenCompose(Function.identity());
   return acknowledgeCompletableFuture.thenApply(
      (Acknowledge ignored) -> new JobSubmissionResult(jobGraph.getJobID()));
}
```

Dispatcher类submit方法，

```
public CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, Time timeout) {
   log.info("Received JobGraph submission {} ({}).", jobGraph.getJobID(), jobGraph.getName());

   try {
   //重复提交的job抛出异常
      if (isDuplicateJob(jobGraph.getJobID())) {
         return FutureUtils.completedExceptionally(
            new DuplicateJobSubmissionException(jobGraph.getJobID()));
            //不完整的job抛出异常
      } else if (isPartialResourceConfigured(jobGraph)) {
         return FutureUtils.completedExceptionally(
            new JobSubmissionException(jobGraph.getJobID(), "Currently jobs is not supported if parts of the vertices have " +
                  "resources configured. The limitation will be removed in future versions."));
      } else {
         return internalSubmitJob(jobGraph);
      }
   } catch (FlinkException e) {
      return FutureUtils.completedExceptionally(e);
   }
}
```

进入Dispatcher类internalSubmitJob方法

```
private CompletableFuture<Acknowledge> internalSubmitJob(JobGraph jobGraph) {
   log.info("Submitting job {} ({}).", jobGraph.getJobID(), jobGraph.getName());

//终止老的JobManager并提交新的job
   final CompletableFuture<Acknowledge> persistAndRunFuture = waitForTerminatingJobManager(jobGraph.getJobID(), jobGraph, this::persistAndRunJob)
      .thenApply(ignored -> Acknowledge.get());

   return persistAndRunFuture.handleAsync((acknowledge, throwable) -> {
      if (throwable != null) {
         cleanUpJobData(jobGraph.getJobID(), true);

         final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);
         log.error("Failed to submit job {}.", jobGraph.getJobID(), strippedThrowable);
         throw new CompletionException(
            new JobSubmissionException(jobGraph.getJobID(), "Failed to submit job.", strippedThrowable));
      } else {
         return acknowledge;
      }
   }, getRpcService().getExecutor());
}
```

看一下waitForTerminatingJobManager方法，就是终止相同jobId的JobManager并启动新的jobmanager，通过执行action.apply(jobGraph)，action方法是persistAndRunJob

```
private CompletableFuture<Void> waitForTerminatingJobManager(JobID jobId, JobGraph jobGraph, FunctionWithException<JobGraph, CompletableFuture<Void>, ?> action) {
   final CompletableFuture<Void> jobManagerTerminationFuture = getJobTerminationFuture(jobId)
      .exceptionally((Throwable throwable) -> {
         throw new CompletionException(
            new DispatcherException(
               String.format("Termination of previous JobManager for job %s failed. Cannot submit job under the same job id.", jobId),
               throwable)); });

   return jobManagerTerminationFuture.thenComposeAsync(
      FunctionUtils.uncheckedFunction((ignored) -> {
         jobManagerTerminationFutures.remove(jobId);
         return action.apply(jobGraph);
      }),
      getMainThreadExecutor());
}
```

以下过程大概是提交job之后，创建JobManager，在JobManager中创建JobMaster，在JobMaster中创建Scheduler，由Scheduler创建ExecutionGraph

```
private CompletableFuture<Void> persistAndRunJob(JobGraph jobGraph) throws Exception {
   jobGraphWriter.putJobGraph(jobGraph);

   final CompletableFuture<Void> runJobFuture = runJob(jobGraph);

   ......
}



private CompletableFuture<Void> runJob(JobGraph jobGraph) {
		//这里创建JobManager
		final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture = createJobManagerRunner(jobGraph);

		jobManagerRunnerFutures.put(jobGraph.getJobID(), jobManagerRunnerFuture);

//创建成功启动JobManager
		return jobManagerRunnerFuture
			.thenApply(FunctionUtils.uncheckedFunction(this::startJobManagerRunner))
			.thenApply(FunctionUtils.nullFn())
			......
	}
	
	
	
	private CompletableFuture<JobManagerRunner> createJobManagerRunner(JobGraph jobGraph) {
		final RpcService rpcService = getRpcService();

		return CompletableFuture.supplyAsync(
			CheckedSupplier.unchecked(() ->
			//DefaultJobManagerRunnerFactory工厂类创建JobManager
				jobManagerRunnerFactory.createJobManagerRunner(
					jobGraph,
					......),
			rpcService.getExecutor());
	}
	
	
	
	public enum DefaultJobManagerRunnerFactory implements JobManagerRunnerFactory {
	INSTANCE;
	@Override
	public JobManagerRunner createJobManagerRunner(
			JobGraph jobGraph,
			......) throws Exception {
			
//DefaultJobMasterServiceFactory工厂类用来创建JobMaster
		final JobMasterServiceFactory jobMasterFactory = new DefaultJobMasterServiceFactory(
			......);

		return new JobManagerRunnerImpl(
			jobGraph,
			......);
	}
}



//JobManagerRunnerImpl创建jobMaster
public JobManagerRunnerImpl(
			final JobGraph jobGraph,
			......) throws Exception {
			
		......
			// now start the JobManager，调用JobMaster的构造函数
			this.jobMasterService = jobMasterFactory.createJobMasterService(jobGraph, this, userCodeLoader);
		......
	}
	
	
	
	//进入JobMaster类构造函数
	public JobMaster(
			......
			JobGraph jobGraph,
			......) throws Exception {

		......
		//创建flink job的调度类，通过DefaultSchedulerFactory工厂类的createInstance创建实例
		this.schedulerNG = createScheduler(jobManagerJobMetricGroup);
		......
	}
	
	
	
	//获取DefaultSchedulerFactory的DefaultScheduler
	public class DefaultSchedulerFactory implements SchedulerNGFactory {

	@Override
	public SchedulerNG createInstance(
	       final JobGraph jobGraph,
			......) throws Exception {
	
      ......
		return new DefaultScheduler(jobGraph,......);
	}
	
	
	
	
	//DefaultScheduler继承自SchedulerBase类，初始化DefaultScheduler要先初始化SchedulerBase
	public SchedulerBase(
		final JobGraph jobGraph,
		......) throws Exception {

		......
		//到这里终于开始创建ExecutionGraph
		this.executionGraph = createAndRestoreExecutionGraph(jobManagerJobMetricGroup, checkNotNull(shuffleMaster), checkNotNull(partitionTracker));
		......
	}
```

进入ExecutionGraph的创建逻辑SchedulerBase#createExecutionGraph

```
private ExecutionGraph createExecutionGraph(
   JobManagerJobMetricGroup currentJobManagerJobMetricGroup,
   ShuffleMaster<?> shuffleMaster,
   final JobMasterPartitionTracker partitionTracker) throws JobExecutionException, JobException {

   final FailoverStrategy.Factory failoverStrategy = legacyScheduling ?
      FailoverStrategyLoader.loadFailoverStrategy(jobMasterConfiguration, log) :
      new NoOpFailoverStrategy.Factory();

   return ExecutionGraphBuilder.buildGraph(
      null,
      jobGraph,
      jobMasterConfiguration,
      futureExecutor,
      ioExecutor,
      slotProvider,
      userCodeLoader,
      checkpointRecoveryFactory,
      rpcTimeout,
      restartStrategy,
      currentJobManagerJobMetricGroup,
      blobWriter,
      slotRequestTimeout,
      log,
      shuffleMaster,
      partitionTracker,
      failoverStrategy);
}
```

关键方法是ExecutionGraphBuilder.buildGraph

```
public static ExecutionGraph buildGraph(
   @Nullable ExecutionGraph prior,
   JobGraph jobGraph,
   Configuration jobManagerConfig,
   ScheduledExecutorService futureExecutor,
   Executor ioExecutor,
   SlotProvider slotProvider,
   ClassLoader classLoader,
   CheckpointRecoveryFactory recoveryFactory,
   Time rpcTimeout,
   RestartStrategy restartStrategy,
   MetricGroup metrics,
   BlobWriter blobWriter,
   Time allocationTimeout,
   Logger log,
   ShuffleMaster<?> shuffleMaster,
   JobMasterPartitionTracker partitionTracker,
   FailoverStrategy.Factory failoverStrategyFactory) throws JobExecutionException, JobException {

   checkNotNull(jobGraph, "job graph cannot be null");

   final String jobName = jobGraph.getName();
   final JobID jobId = jobGraph.getJobID();

//创建JobInformation
   final JobInformation jobInformation = new JobInformation(
      jobId,
      jobName,
      jobGraph.getSerializedExecutionConfig(),
      jobGraph.getJobConfiguration(),
      jobGraph.getUserJarBlobKeys(),
      jobGraph.getClasspaths());

   final int maxPriorAttemptsHistoryLength =
         jobManagerConfig.getInteger(JobManagerOptions.MAX_ATTEMPTS_HISTORY_SIZE);

   final PartitionReleaseStrategy.Factory partitionReleaseStrategyFactory =
      PartitionReleaseStrategyFactoryLoader.loadPartitionReleaseStrategyFactory(jobManagerConfig);

   // create a new execution graph, if none exists so far
   //新建ExecutionGraph类
   final ExecutionGraph executionGraph;
   try {
      executionGraph = (prior != null) ? prior :
         new ExecutionGraph(
            jobInformation,
            futureExecutor,
            ioExecutor,
            rpcTimeout,
            restartStrategy,
            maxPriorAttemptsHistoryLength,
            failoverStrategyFactory,
            slotProvider,
            classLoader,
            blobWriter,
            allocationTimeout,
            partitionReleaseStrategyFactory,
            shuffleMaster,
            partitionTracker,
            jobGraph.getScheduleMode());
   } catch (IOException e) {
      throw new JobException("Could not create the ExecutionGraph.", e);
   }

   // set the basic properties
//设置JsonPlan
   try {
      executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));
   }
   catch (Throwable t) {
      log.warn("Cannot create JSON plan for job", t);
      // give the graph an empty plan
      executionGraph.setJsonPlan("{}");
   }

   // initialize the vertices that have a master initialization hook
   // file output formats create directories here, input formats create splits

   final long initMasterStart = System.nanoTime();
   log.info("Running initialization on master for job {} ({}).", jobName, jobId);

//遍历JobVertex节点并在Master上初始化，主要关注OutputFormatVertex 和 InputFormatVertex，其他类型的 vertex 在这里没有什么特殊操作。File output format 在这一步准备好输出目录, Input splits 在这一步创建对应的 splits。
   for (JobVertex vertex : jobGraph.getVertices()) {
      String executableClass = vertex.getInvokableClassName();
      if (executableClass == null || executableClass.isEmpty()) {
         throw new JobSubmissionException(jobId,
               "The vertex " + vertex.getID() + " (" + vertex.getName() + ") has no invokable class.");
      }

      try {
         vertex.initializeOnMaster(classLoader);
      }
      catch (Throwable t) {
            throw new JobExecutionException(jobId,
                  "Cannot initialize task '" + vertex.getName() + "': " + t.getMessage(), t);
      }
   }

   log.info("Successfully ran initialization on master in {} ms.",
         (System.nanoTime() - initMasterStart) / 1_000_000);

   // topologically sort the job vertices and attach the graph to the existing one
   //对所有的 Jobvertext 进行拓扑排序，并生成 ExecutionGraph 内部的节点和连接，所谓拓扑排序，即保证如果存在 A -> B 的有向边，那么在排序后的列表中 A 节点一定在 B 节点之前。
   List<JobVertex> sortedTopology = jobGraph.getVerticesSortedTopologicallyFromSources();
   if (log.isDebugEnabled()) {
      log.debug("Adding {} vertices from job graph {} ({}).", sortedTopology.size(), jobName, jobId);
   }
   //核心方法，生成executionGraph的节点
   executionGraph.attachJobGraph(sortedTopology);

   ......(省略的部分均为checkpointing 相关逻辑)

   return executionGraph;
}
```

主要逻辑是ExecutionGraph#attachJobGraph,在创建 `ExecutionJobVertex` 的时候会创建对应的 `ExecutionVertex`， `IntermediateResult`，`ExecutionEdge` ， `IntermediateResultPartition` 等对象，这里涉及到的对象相对较多，概括起来大致是这样的：

- 每一个 `JobVertex` 对应一个 ExecutionJobVertex,
- 每一个 `ExecutionJobVertex` 有 parallelism 个 `ExecutionVertex`
- 每一个 `JobVertex` 可能有 n(n>=0) 个 `IntermediateDataSet`，在 `ExecutionJobVertex` 中，一个 `IntermediateDataSet` 对应一个 `IntermediateResult`, 每一个 `IntermediateResult` 都有 parallelism 个生产者, 对应 parallelism 个`IntermediateResultPartition`
- 每一个 `ExecutionJobVertex` 都会和前向的 `IntermediateResult` 连接，实际上是 `ExecutionVertex`和 `IntermediateResult` 建立连接，生成 `ExecutionEdge`

```
public void attachJobGraph(List<JobVertex> topologiallySorted) throws JobException {

   for (JobVertex jobVertex : topologiallySorted) {

      if (jobVertex.isInputVertex() && !jobVertex.isStoppable()) {
         this.isStoppable = false;
      }

      // create the execution job vertex and attach it to the graph
      //在这里生成ExecutionGraph的每个节点
            //首先是进行了一堆赋值，将任务信息交给要生成的图节点，以及设定并行度等等
            //然后是创建本节点的IntermediateResult，根据本节点的下游节点的个数确定创建几份
            //最后是根据设定好的并行度创建用于执行task的ExecutionVertex
            //如果job有设定inputsplit的话，这里还要指定inputsplits
      ExecutionJobVertex ejv = new ExecutionJobVertex(
            this,
            jobVertex,
            1,
            maxPriorAttemptsHistoryLength,
            rpcTimeout,
            globalModVersion,
            createTimestamp);

//这里要处理所有的JobEdge
            //对每个edge，获取对应的intermediateResult，并记录到本节点的输入上
            //最后，把每个ExecutorVertex和对应的IntermediateResult关联起来
      ejv.connectToPredecessors(this.intermediateResults);

      ExecutionJobVertex previousTask = this.tasks.putIfAbsent(jobVertex.getID(), ejv);
      if (previousTask != null) {
         throw new JobException(String.format("Encountered two job vertices with ID %s : previous=[%s] / new=[%s]",
            jobVertex.getID(), ejv, previousTask));
      }

      for (IntermediateResult res : ejv.getProducedDataSets()) {
         IntermediateResult previousDataSet = this.intermediateResults.putIfAbsent(res.getId(), res);
         if (previousDataSet != null) {
            throw new JobException(String.format("Encountered two intermediate data set with ID %s : previous=[%s] / new=[%s]",
               res.getId(), res, previousDataSet));
         }
      }

      this.verticesInCreationOrder.add(ejv);
      this.numVerticesTotal += ejv.getParallelism();
      newExecJobVertices.add(ejv);
   }

   // the topology assigning should happen before notifying new vertices to failoverStrategy
   executionTopology = new DefaultExecutionTopology(this);

   failoverStrategy.notifyNewVertices(newExecJobVertices);

   partitionReleaseStrategy = partitionReleaseStrategyFactory.createInstance(getSchedulingTopology());
}
```

ExecutionJobVertex,在 `ExecutionGraph` 中，节点对应的类是 `ExecutionJobVertex`，与之对应的就是 `JobGraph` 中的 `JobVertex`。每一个 `ExexutionJobVertex` 都是由一个 `JobVertex` 生成的。

```
public class ExecutionJobVertex implements AccessExecutionJobVertex, Archiveable<ArchivedExecutionJobVertex> {

	/** Use the same log for all ExecutionGraph classes. */
	private static final Logger LOG = ExecutionGraph.LOG;

	public static final int VALUE_NOT_SET = -1;

	private final Object stateMonitor = new Object();

	private final ExecutionGraph graph;

	private final JobVertex jobVertex;

	/**
	 * The IDs of all operators contained in this execution job vertex.
	 *
	 * <p>The ID's are stored depth-first post-order; for the forking chain below the ID's would be stored as [D, E, B, C, A].
	 *  A - B - D
	 *   \    \
	 *    C    E
	 * This is the same order that operators are stored in the {@code StreamTask}.
	 */
	private final List<OperatorID> operatorIDs;

	/**
	 * The alternative IDs of all operators contained in this execution job vertex.
	 *
	 * <p>The ID's are in the same order as {@link ExecutionJobVertex#operatorIDs}.
	 */
	private final List<OperatorID> userDefinedOperatorIds;

	private final ExecutionVertex[] taskVertices;

	private final IntermediateResult[] producedDataSets;

	private final List<IntermediateResult> inputs;
}

	
public ExecutionJobVertex(
      ExecutionGraph graph,
      JobVertex jobVertex,
      int defaultParallelism,
      int maxPriorAttemptsHistoryLength,
      Time timeout,
      long initialGlobalModVersion,
      long createTimestamp) throws JobException {

   if (graph == null || jobVertex == null) {
      throw new NullPointerException();
   }

   this.graph = graph;
   this.jobVertex = jobVertex;

   int vertexParallelism = jobVertex.getParallelism();
   int numTaskVertices = vertexParallelism > 0 ? vertexParallelism : defaultParallelism;

   final int configuredMaxParallelism = jobVertex.getMaxParallelism();

   this.maxParallelismConfigured = (VALUE_NOT_SET != configuredMaxParallelism);

   // if no max parallelism was configured by the user, we calculate and set a default
   setMaxParallelismInternal(maxParallelismConfigured ?
         configuredMaxParallelism : KeyGroupRangeAssignment.computeDefaultMaxParallelism(numTaskVertices));

   // verify that our parallelism is not higher than the maximum parallelism
   if (numTaskVertices > maxParallelism) {
      throw new JobException(
         String.format("Vertex %s's parallelism (%s) is higher than the max parallelism (%s). Please lower the parallelism or increase the max parallelism.",
            jobVertex.getName(),
            numTaskVertices,
            maxParallelism));
   }

   this.parallelism = numTaskVertices;
   this.resourceProfile = ResourceProfile.fromResourceSpec(jobVertex.getMinResources(), MemorySize.ZERO);

   this.taskVertices = new ExecutionVertex[numTaskVertices];
   this.operatorIDs = Collections.unmodifiableList(jobVertex.getOperatorIDs());
   this.userDefinedOperatorIds = Collections.unmodifiableList(jobVertex.getUserDefinedOperatorIDs());

   this.inputs = new ArrayList<>(jobVertex.getInputs().size());

   // take the sharing group
   this.slotSharingGroup = jobVertex.getSlotSharingGroup();
   this.coLocationGroup = jobVertex.getCoLocationGroup();

   // setup the coLocation group
   if (coLocationGroup != null && slotSharingGroup == null) {
      throw new JobException("Vertex uses a co-location constraint without using slot sharing");
   }

   // create the intermediate results
   //根据jobGraph的IntermediateDataSet生成IntermediateResult，但是会传入并行度参数numTaskVertices，根据并行度在IntermediateResult内部会生成numTaskVertices个IntermediateResultPartition分区。
   
   this.producedDataSets = new IntermediateResult[jobVertex.getNumberOfProducedIntermediateDataSets()];

   for (int i = 0; i < jobVertex.getProducedDataSets().size(); i++) {
      final IntermediateDataSet result = jobVertex.getProducedDataSets().get(i);

      this.producedDataSets[i] = new IntermediateResult(
            result.getId(),
            this,
            numTaskVertices,
            result.getResultType());
   }

   // create all task vertices
   //一个ExecutionJobVertex对应numTaskVertices个ExecutionVertex节点
   for (int i = 0; i < numTaskVertices; i++) {
      ExecutionVertex vertex = new ExecutionVertex(
            this,
            i,
            producedDataSets,
            timeout,
            initialGlobalModVersion,
            createTimestamp,
            maxPriorAttemptsHistoryLength);

      this.taskVertices[i] = vertex;
   }

   // sanity check for the double referencing between intermediate result partitions and execution vertices
   for (IntermediateResult ir : this.producedDataSets) {
      if (ir.getNumberOfAssignedPartitions() != parallelism) {
         throw new RuntimeException("The intermediate result's partitions were not correctly assigned.");
      }
   }

   // set up the input splits, if the vertex has any
   try {
      @SuppressWarnings("unchecked")
      InputSplitSource<InputSplit> splitSource = (InputSplitSource<InputSplit>) jobVertex.getInputSplitSource();

      if (splitSource != null) {
         Thread currentThread = Thread.currentThread();
         ClassLoader oldContextClassLoader = currentThread.getContextClassLoader();
         currentThread.setContextClassLoader(graph.getUserClassLoader());
         try {
            inputSplits = splitSource.createInputSplits(numTaskVertices);

            if (inputSplits != null) {
               splitAssigner = splitSource.getInputSplitAssigner(inputSplits);
            }
         } finally {
            currentThread.setContextClassLoader(oldContextClassLoader);
         }
      }
      else {
         inputSplits = null;
      }
   }
   catch (Throwable t) {
      throw new JobException("Creating the input splits caused an error: " + t.getMessage(), t);
   }
}
```

由于 `ExecutionJobVertex` 有 numParallelProducers 个并行的子任务，自然对应的每一个 `IntermediateResult` 就有 numParallelProducers 个生产者，每个生产者的在相应的 `IntermediateResult`上的输出对应一个 `IntermediateResultPartition`。`IntermediateResultPartition` 表示的是 `ExecutionVertex` 的一个输出分区，

一个 `ExecutionJobVertex` 可能包含多个（n） 个 `IntermediateResult`， 那实际上每一个并行的子任务 `ExecutionVertex` 可能会会包含（n） 个 `IntermediateResultPartition`。

IntermediateResultPartition` 的生产者是 `ExecutionVertex`，消费者是一个或若干个 `ExecutionEdge

```
public class IntermediateResult {

   private final IntermediateDataSetID id;

   private final ExecutionJobVertex producer;

   private final IntermediateResultPartition[] partitions;

   /**
    * Maps intermediate result partition IDs to a partition index. This is
    * used for ID lookups of intermediate results. I didn't dare to change the
    * partition connect logic in other places that is tightly coupled to the
    * partitions being held as an array.
    */
   private final HashMap<IntermediateResultPartitionID, Integer> partitionLookupHelper = new HashMap<>();

   private final int numParallelProducers;

   private final AtomicInteger numberOfRunningProducers;

   private int partitionsAssigned;

   private int numConsumers;

   private final int connectionIndex;

   private final ResultPartitionType resultType;
```

`ExexutionJobVertex` 的成员变量中包含一个 `ExecutionVertex` 数组。我们知道，Flink Job 是可以指定任务的并行度的，在实际运行时，会有多个并行的任务同时在执行，对应到这里就是 `ExecutionVertex`。`ExecutionVertex` 是并行任务的一个子任务，算子的并行度是多少，那么就会有多少个 `ExecutionVertex`。

```
public class ExecutionVertex implements AccessExecutionVertex, Archiveable<ArchivedExecutionVertex> {

	private final ExecutionJobVertex jobVertex;

	private final Map<IntermediateResultPartitionID, IntermediateResultPartition> resultPartitions;

	private final ExecutionEdge[][] inputEdges;

	private final int subTaskIndex;

	private final ExecutionVertexID executionVertexId;
}

public ExecutionVertex(
      ExecutionJobVertex jobVertex,
      int subTaskIndex,
      IntermediateResult[] producedDataSets,
      Time timeout,
      long initialGlobalModVersion,
      long createTimestamp,
      int maxPriorExecutionHistoryLength) {

   this.jobVertex = jobVertex;
   this.subTaskIndex = subTaskIndex;
   this.executionVertexId = new ExecutionVertexID(jobVertex.getJobVertexId(), subTaskIndex);
   this.taskNameWithSubtask = String.format("%s (%d/%d)",
         jobVertex.getJobVertex().getName(), subTaskIndex + 1, jobVertex.getParallelism());

   this.resultPartitions = new LinkedHashMap<>(producedDataSets.length, 1);

//对每个下游IntermediateResult，都创建并行度数量的分区IntermediateResultPartition
   for (IntermediateResult result : producedDataSets) {
      IntermediateResultPartition irp = new IntermediateResultPartition(result, this, subTaskIndex);
      result.setPartition(subTaskIndex, irp);

      resultPartitions.put(irp.getPartitionId(), irp);
   }

//初始化边ExecutionEdge
   this.inputEdges = new ExecutionEdge[jobVertex.getJobVertex().getInputs().size()][];

   this.priorExecutions = new EvictingBoundedList<>(maxPriorExecutionHistoryLength);

//初始化Execution，Execution 是对 ExecutionVertex 的一次执行，通过 ExecutionAttemptId 来唯一标识。
   this.currentExecution = new Execution(
      getExecutionGraph().getFutureExecutor(),
      this,
      0,
      initialGlobalModVersion,
      createTimestamp,
      timeout);

   // create a co-location scheduling hint, if necessary
   CoLocationGroup clg = jobVertex.getCoLocationGroup();
   if (clg != null) {
      this.locationConstraint = clg.getLocationConstraint(subTaskIndex);
   }
   else {
      this.locationConstraint = null;
   }

   getExecutionGraph().registerExecution(currentExecution);

   this.timeout = timeout;
   this.inputSplits = new ArrayList<>();
}
```

`ExecutionEdge` 表示 `ExecutionVertex` 的输入，通过 `ExecutionEdge` 将 `ExecutionVertex` 和 `IntermediateResultPartition` 连接起来，进而在不同的 `ExecutionVertex` 之间建立联系。

```
public class ExecutionEdge {

   private final IntermediateResultPartition source;

   private final ExecutionVertex target;

   private final int inputNum;
   }
```