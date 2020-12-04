回顾之前JobManager的启动过程，创建好JobManagerRunner之后，调用Dispatcher的startJobManagerRunner启动JobManager。

```
private JobManagerRunner startJobManagerRunner(JobManagerRunner jobManagerRunner) throws Exception {
   final JobID jobId = jobManagerRunner.getJobID();

   FutureUtils.assertNoException(
      jobManagerRunner.getResultFuture().handleAsync(
         (ArchivedExecutionGraph archivedExecutionGraph, Throwable throwable) -> {
            // check if we are still the active JobManagerRunner by checking the identity
            final JobManagerRunner currentJobManagerRunner = Optional.ofNullable(jobManagerRunnerFutures.get(jobId))
               .map(future -> future.getNow(null))
               .orElse(null);
            //noinspection ObjectEquality
            if (jobManagerRunner == currentJobManagerRunner) {
               if (archivedExecutionGraph != null) {
                  jobReachedGloballyTerminalState(archivedExecutionGraph);
               } else {
                  final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);

                  if (strippedThrowable instanceof JobNotFinishedException) {
                     jobNotFinished(jobId);
                  } else {
                     jobMasterFailed(jobId, strippedThrowable);
                  }
               }
            } else {
               log.debug("There is a newer JobManagerRunner for the job {}.", jobId);
            }

            return null;
         }, getMainThreadExecutor()));
//启动jobManager
   jobManagerRunner.start();

   return jobManagerRunner;
}
```

JobManagerRunnerImpl类start

```
public void start() throws Exception {
   try {
   //选举leader节点
      leaderElectionService.start(this);
   } catch (Exception e) {
      log.error("Could not start the JobManager because the leader election service did not start.", e);
      throw new Exception("Could not start the leader election service.", e);
   }
}
```

主节点选举成功回调JobManagerRunnerImpl类的grantLeadership回调方法

```
public void grantLeadership(final UUID leaderSessionID) {
   synchronized (lock) {
      if (shutdown) {
         log.info("JobManagerRunner already shutdown.");
         return;
      }

      leadershipOperation = leadershipOperation.thenCompose(
         (ignored) -> {
            synchronized (lock) {
               return verifyJobSchedulingStatusAndStartJobManager(leaderSessionID);
            }
         });

      handleException(leadershipOperation, "Could not start the job manager.");
   }
}


private CompletableFuture<Void> verifyJobSchedulingStatusAndStartJobManager(UUID leaderSessionId) {
		final CompletableFuture<JobSchedulingStatus> jobSchedulingStatusFuture = getJobSchedulingStatus();

		return jobSchedulingStatusFuture.thenCompose(
			jobSchedulingStatus -> {
				if (jobSchedulingStatus == JobSchedulingStatus.DONE) {
					return jobAlreadyDone();
				} else {
				//启动startJobMaster
					return startJobMaster(leaderSessionId);
				}
			});
	}
```

主节点选举成功启动startJobMaster

```
private CompletionStage<Void> startJobMaster(UUID leaderSessionId) {
   log.info("JobManager runner for job {} ({}) was granted leadership with session id {} at {}.",
      jobGraph.getName(), jobGraph.getJobID(), leaderSessionId, jobMasterService.getAddress());

   try {
      runningJobsRegistry.setJobRunning(jobGraph.getJobID());
   } catch (IOException e) {
      return FutureUtils.completedExceptionally(
         new FlinkException(
            String.format("Failed to set the job %s to running in the running jobs registry.", jobGraph.getJobID()),
            e));
   }

   final CompletableFuture<Acknowledge> startFuture;
   try {
   //使用leaderSessionId构建 JobMasterId 启动 JobMaster
      startFuture = jobMasterService.start(new JobMasterId(leaderSessionId));
   } catch (Exception e) {
      return FutureUtils.completedExceptionally(new FlinkException("Failed to start the JobMaster.", e));
   }

   final CompletableFuture<JobMasterGateway> currentLeaderGatewayFuture = leaderGatewayFuture;
   return startFuture.thenAcceptAsync(
      (Acknowledge ack) -> confirmLeaderSessionIdIfStillLeader(
         leaderSessionId,
         jobMasterService.getAddress(),
         currentLeaderGatewayFuture),
      executor);
}
```

进入JobMaster类start

```
public CompletableFuture<Acknowledge> start(final JobMasterId newJobMasterId) throws Exception {
   // make sure we receive RPC and async calls
   //启动RpcServer
   start();

   return callAsyncWithoutFencing(() -> startJobExecution(newJobMasterId), RpcUtils.INF_TIMEOUT);
}
```

```
private Acknowledge startJobExecution(JobMasterId newJobMasterId) throws Exception {

   validateRunsInMainThread();

   checkNotNull(newJobMasterId, "The new JobMasterId must not be null.");

   if (Objects.equals(getFencingToken(), newJobMasterId)) {
      log.info("Already started the job execution with JobMasterId {}.", newJobMasterId);

      return Acknowledge.get();
   }

   setNewFencingToken(newJobMasterId);

   startJobMasterServices();

   log.info("Starting execution of job {} ({}) under job master id {}.", jobGraph.getName(), jobGraph.getJobID(), newJobMasterId);

   resetAndStartScheduler();

   return Acknowledge.get();
}
```

startJobMasterServices与ResourceManager建立连接，之后resetAndStartScheduler进入job的调度流程。

```
private void resetAndStartScheduler() throws Exception {
   ......

   schedulerAssignedFuture.thenRun(this::startScheduling);
}
```

startScheduling

```
private void startScheduling() {
   checkState(jobStatusListener == null);
   // register self as job status change listener
   //状态监听
   jobStatusListener = new JobManagerJobStatusListener();
   schedulerNG.registerJobStatusListener(jobStatusListener);

   schedulerNG.startScheduling();
}
```

SchedulerBase类startScheduling

```
public final void startScheduling() {
   mainThreadExecutor.assertRunningInMainThread();
   //注册监控
   registerJobMetrics();
   startSchedulingInternal();
}
```

LegacyScheduler类startSchedulingInternal

```
protected void startSchedulingInternal() {
   final ExecutionGraph executionGraph = getExecutionGraph();
   try {
      executionGraph.scheduleForExecution();
   }
   catch (Throwable t) {
      executionGraph.failGlobal(t);
   }
}
```

ExecutionGraph类的scheduleForExecution

```
public void scheduleForExecution() throws JobException {

   assertRunningInJobMasterMainThread();

   if (isLegacyScheduling()) {
      LOG.info("Job recovers via failover strategy: {}", failoverStrategy.getStrategyName());
   }

   final long currentGlobalModVersion = globalModVersion;

//将job状态由CREATED转为RUNNING
   if (transitionState(JobStatus.CREATED, JobStatus.RUNNING)) {

      final CompletableFuture<Void> newSchedulingFuture = SchedulingUtils.schedule(
         scheduleMode,
         getAllExecutionVertices(),
         this);

      if (state == JobStatus.RUNNING && currentGlobalModVersion == globalModVersion) {
         schedulingFuture = newSchedulingFuture;
         newSchedulingFuture.whenComplete(
            (Void ignored, Throwable throwable) -> {
               if (throwable != null) {
                  final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);

                  if (!(strippedThrowable instanceof CancellationException)) {
                     // only fail if the scheduling future was not canceled
                     failGlobal(strippedThrowable);
                  }
               }
            });
      } else {
         newSchedulingFuture.cancel(false);
      }
   }
   else {
      throw new IllegalStateException("Job may only be scheduled from state " + JobStatus.CREATED);
   }
}
```



```
public static CompletableFuture<Void> schedule(
      ScheduleMode scheduleMode,
      final Iterable<ExecutionVertex> vertices,
      final ExecutionGraph executionGraph) {

   switch (scheduleMode) {
   //只运行 source，其它的子任务由source进行通知
      case LAZY_FROM_SOURCES:
      case LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST:
         return scheduleLazy(vertices, executionGraph);

//所有的子任务都立即进行调度，这是 streaming 模式采用的方式
      case EAGER:
         return scheduleEager(vertices, executionGraph);

      default:
         throw new IllegalStateException(String.format("Schedule mode %s is invalid.", scheduleMode));
   }
}
```

scheduleEager调度子任务

```
public static CompletableFuture<Void> scheduleEager(
      final Iterable<ExecutionVertex> vertices,
      final ExecutionGraph executionGraph) {

   executionGraph.assertRunningInJobMasterMainThread();

   checkState(executionGraph.getState() == JobStatus.RUNNING, "job is not running currently");

   // Important: reserve all the space we need up front.
   // that way we do not have any operation that can fail between allocating the slots
   // and adding them to the list. If we had a failure in between there, that would
   // cause the slots to get lost

   // collecting all the slots may resize and fail in that operation without slots getting lost
   final ArrayList<CompletableFuture<Execution>> allAllocationFutures = new ArrayList<>();

   final SlotProviderStrategy slotProviderStrategy = executionGraph.getSlotProviderStrategy();
   final Set<AllocationID> allPreviousAllocationIds = Collections.unmodifiableSet(
      computePriorAllocationIdsIfRequiredByScheduling(vertices, slotProviderStrategy.asSlotProvider()));

//分配已slot资源
   // allocate the slots (obtain all their futures)
   for (ExecutionVertex ev : vertices) {
      // these calls are not blocking, they only return futures
      CompletableFuture<Execution> allocationFuture = ev.getCurrentExecutionAttempt().allocateResourcesForExecution(
         slotProviderStrategy,
         LocationPreferenceConstraint.ALL,
         allPreviousAllocationIds);

      allAllocationFutures.add(allocationFuture);
   }

   // this future is complete once all slot futures are complete.
   // the future fails once one slot future fails.
   // 等待所有需要调度的子任务都分配到资源
   final ConjunctFuture<Collection<Execution>> allAllocationsFuture = FutureUtils.combineAll(allAllocationFutures);

   return allAllocationsFuture.thenAccept(
      (Collection<Execution> executionsToDeploy) -> {
         for (Execution execution : executionsToDeploy) {
            try {
            //启动 Execution
               execution.deploy();
            } catch (Throwable t) {
               throw new CompletionException(
                  new FlinkException(
                     String.format("Could not deploy execution %s.", execution),
                     t));
            }
         }
      })
      // Generate a more specific failure message for the eager scheduling
      .exceptionally(......);
}
```

`Execution` 是 `ExecutionVertex` 的一次执行，在调度的时候会先生成对任务的描述 `TaskDeploymentDescription`， `TaskDeploymentDescription` 包含了对输入的描述 `InputGateDeploymentDescriptor`, 对输出的描述 `ResultPartitionDeploymentDescriptor`，以及保存了这个 Task 中运行的所有算子运行时信息的 `TaskInformation` 和 `JobInformation`。生成了 `TaskDeploymentDescription` 通过 RPC 调用提交给 `TaskExecutor` 执行。

Execution 类的deploy

```
public void deploy() throws JobException {
   assertRunningInJobMasterMainThread();

   final LogicalSlot slot  = assignedResource;

   checkNotNull(slot, "In order to deploy the execution we first have to assign a resource via tryAssignResource.");

   // Check if the TaskManager died in the meantime
   // This only speeds up the response to TaskManagers failing concurrently to deployments.
   // The more general check is the rpcTimeout of the deployment call
   if (!slot.isAlive()) {
      throw new JobException("Target slot (TaskManager) for deployment is no longer alive.");
   }

   // make sure exactly one deployment call happens from the correct state
   // note: the transition from CREATED to DEPLOYING is for testing purposes only
   ExecutionState previous = this.state;
   if (previous == SCHEDULED || previous == CREATED) {
      if (!transitionState(previous, DEPLOYING)) {
         // race condition, someone else beat us to the deploying call.
         // this should actually not happen and indicates a race somewhere else
         throw new IllegalStateException("Cannot deploy task: Concurrent deployment call race.");
      }
   }
   else {
      // vertex may have been cancelled, or it was already scheduled
      throw new IllegalStateException("The vertex must be in CREATED or SCHEDULED state to be deployed. Found state " + previous);
   }

   if (this != slot.getPayload()) {
      throw new IllegalStateException(
         String.format("The execution %s has not been assigned to the assigned slot.", this));
   }

   try {

      // race double check, did we fail/cancel and do we need to release the slot?
      if (this.state != DEPLOYING) {
         slot.releaseSlot(new FlinkException("Actual state of execution " + this + " (" + state + ") does not match expected state DEPLOYING."));
         return;
      }

      if (LOG.isInfoEnabled()) {
         LOG.info(String.format("Deploying %s (attempt #%d) to %s", vertex.getTaskNameWithSubtaskIndex(),
               attemptNumber, getAssignedResourceLocation()));
      }

//TaskDeploymentDescription 是对任务的描述，包含了task运行需要所以信息
      final TaskDeploymentDescriptor deployment = TaskDeploymentDescriptorFactory
         .fromExecutionVertex(vertex, attemptNumber)
         .createDeploymentDescriptor(
            slot.getAllocationId(),
            slot.getPhysicalSlotNumber(),
            taskRestore,
            producedPartitions.values());

      // null taskRestore to let it be GC'ed
      taskRestore = null;

      final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

      final ComponentMainThreadExecutor jobMasterMainThreadExecutor =
         vertex.getExecutionGraph().getJobMasterMainThreadExecutor();

      // We run the submission in the future executor so that the serialization of large TDDs does not block
      // the main thread and sync back to the main thread once submission is completed.
      //向taskManagerGateway提交job
      CompletableFuture.supplyAsync(() -> taskManagerGateway.submitTask(deployment, rpcTimeout), executor)
         .thenCompose(Function.identity())
         .whenCompleteAsync(
            ......);

   }
   ......
}
```

taskManagerGateway最终调用TaskExecutor的submitTask

```
public CompletableFuture<Acknowledge> submitTask(
      TaskDeploymentDescriptor tdd,
      JobMasterId jobMasterId,
      Time timeout) {

   try {
      final JobID jobId = tdd.getJobId();
      final JobManagerConnection jobManagerConnection = jobManagerTable.get(jobId);

      if (jobManagerConnection == null) {
         final String message = "Could not submit task because there is no JobManager " +
            "associated for the job " + jobId + '.';

         log.debug(message);
         throw new TaskSubmissionException(message);
      }

      if (!Objects.equals(jobManagerConnection.getJobMasterId(), jobMasterId)) {
         final String message = "Rejecting the task submission because the job manager leader id " +
            jobMasterId + " does not match the expected job manager leader id " +
            jobManagerConnection.getJobMasterId() + '.';

         log.debug(message);
         throw new TaskSubmissionException(message);
      }

      if (!taskSlotTable.tryMarkSlotActive(jobId, tdd.getAllocationId())) {
         final String message = "No task slot allocated for job ID " + jobId +
            " and allocation ID " + tdd.getAllocationId() + '.';
         log.debug(message);
         throw new TaskSubmissionException(message);
      }

      // re-integrate offloaded data:
      try {
         tdd.loadBigData(blobCacheService.getPermanentBlobService());
      } catch (IOException | ClassNotFoundException e) {
         throw new TaskSubmissionException("Could not re-integrate offloaded TaskDeploymentDescriptor data.", e);
      }

      // deserialize the pre-serialized information
      final JobInformation jobInformation;
      final TaskInformation taskInformation;
      try {
         jobInformation = tdd.getSerializedJobInformation().deserializeValue(getClass().getClassLoader());
         taskInformation = tdd.getSerializedTaskInformation().deserializeValue(getClass().getClassLoader());
      } catch (IOException | ClassNotFoundException e) {
         throw new TaskSubmissionException("Could not deserialize the job or task information.", e);
      }

      if (!jobId.equals(jobInformation.getJobId())) {
         throw new TaskSubmissionException(
            "Inconsistent job ID information inside TaskDeploymentDescriptor (" +
               tdd.getJobId() + " vs. " + jobInformation.getJobId() + ")");
      }

      TaskMetricGroup taskMetricGroup = taskManagerMetricGroup.addTaskForJob(
         jobInformation.getJobId(),
         jobInformation.getJobName(),
         taskInformation.getJobVertexId(),
         tdd.getExecutionAttemptId(),
         taskInformation.getTaskName(),
         tdd.getSubtaskIndex(),
         tdd.getAttemptNumber());

      InputSplitProvider inputSplitProvider = new RpcInputSplitProvider(
         jobManagerConnection.getJobManagerGateway(),
         taskInformation.getJobVertexId(),
         tdd.getExecutionAttemptId(),
         taskManagerConfiguration.getTimeout());

      TaskManagerActions taskManagerActions = jobManagerConnection.getTaskManagerActions();
      CheckpointResponder checkpointResponder = jobManagerConnection.getCheckpointResponder();
      GlobalAggregateManager aggregateManager = jobManagerConnection.getGlobalAggregateManager();

      LibraryCacheManager libraryCache = jobManagerConnection.getLibraryCacheManager();
      ResultPartitionConsumableNotifier resultPartitionConsumableNotifier = jobManagerConnection.getResultPartitionConsumableNotifier();
      PartitionProducerStateChecker partitionStateChecker = jobManagerConnection.getPartitionStateChecker();

//Task本地状态保存
      final TaskLocalStateStore localStateStore = localStateStoresManager.localStateStoreForSubtask(
         jobId,
         tdd.getAllocationId(),
         taskInformation.getJobVertexId(),
         tdd.getSubtaskIndex());

      final JobManagerTaskRestore taskRestore = tdd.getTaskRestore();

//Task状态管理
      final TaskStateManager taskStateManager = new TaskStateManagerImpl(
         jobId,
         tdd.getExecutionAttemptId(),
         localStateStore,
         taskRestore,
         checkpointResponder);

      MemoryManager memoryManager;
      try {
      //task内存管理
         memoryManager = taskSlotTable.getTaskMemoryManager(tdd.getAllocationId());
      } catch (SlotNotFoundException e) {
         throw new TaskSubmissionException("Could not submit task.", e);
      }

//taskmanager初始化task
      Task task = new Task(
         jobInformation,
         taskInformation,
         tdd.getExecutionAttemptId(),
         tdd.getAllocationId(),
         tdd.getSubtaskIndex(),
         tdd.getAttemptNumber(),
         tdd.getProducedPartitions(),
         tdd.getInputGates(),
         tdd.getTargetSlotNumber(),
         memoryManager,
         taskExecutorServices.getIOManager(),
         taskExecutorServices.getShuffleEnvironment(),
         taskExecutorServices.getKvStateService(),
         taskExecutorServices.getBroadcastVariableManager(),
         taskExecutorServices.getTaskEventDispatcher(),
         taskStateManager,
         taskManagerActions,
         inputSplitProvider,
         checkpointResponder,
         aggregateManager,
         blobCacheService,
         libraryCache,
         fileCache,
         taskManagerConfiguration,
         taskMetricGroup,
         resultPartitionConsumableNotifier,
         partitionStateChecker,
         getRpcService().getExecutor());

      taskMetricGroup.gauge(MetricNames.IS_BACKPRESSURED, task::isBackPressured);

      log.info("Received task {}.", task.getTaskInfo().getTaskNameWithSubtasks());

      boolean taskAdded;

      try {
         taskAdded = taskSlotTable.addTask(task);
      } catch (SlotNotFoundException | SlotNotActiveException e) {
         throw new TaskSubmissionException("Could not submit task.", e);
      }

      if (taskAdded) {
      //启动task
         task.startTaskThread();

         setupResultPartitionBookkeeping(
            tdd.getJobId(),
            tdd.getProducedPartitions(),
            task.getTerminationFuture());
         return CompletableFuture.completedFuture(Acknowledge.get());
      } else {
         final String message = "TaskManager already contains a task for id " +
            task.getExecutionId() + '.';

         log.debug(message);
         throw new TaskSubmissionException(message);
      }
   } catch (TaskSubmissionException e) {
      return FutureUtils.completedExceptionally(e);
   }
}
```

初始化Task

```
public class Task implements Runnable, TaskSlotPayload, TaskActions, PartitionProducerStateProvider, CheckpointListener, BackPressureSampleableTask {

   ......

   /**
    * <p><b>IMPORTANT:</b> This constructor may not start any work that would need to
    * be undone in the case of a failing task deployment.</p>
    */
   public Task(
      JobInformation jobInformation,
      TaskInformation taskInformation,
      ExecutionAttemptID executionAttemptID,
      AllocationID slotAllocationId,
      int subtaskIndex,
      int attemptNumber,
      Collection<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
      Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors,
      int targetSlotNumber,
      MemoryManager memManager,
      IOManager ioManager,
      ShuffleEnvironment<?, ?> shuffleEnvironment,
      KvStateService kvStateService,
      BroadcastVariableManager bcVarManager,
      TaskEventDispatcher taskEventDispatcher,
      TaskStateManager taskStateManager,
      TaskManagerActions taskManagerActions,
      InputSplitProvider inputSplitProvider,
      CheckpointResponder checkpointResponder,
      GlobalAggregateManager aggregateManager,
      BlobCacheService blobService,
      LibraryCacheManager libraryCache,
      FileCache fileCache,
      TaskManagerRuntimeInfo taskManagerConfig,
      @Nonnull TaskMetricGroup metricGroup,
      ResultPartitionConsumableNotifier resultPartitionConsumableNotifier,
      PartitionProducerStateChecker partitionProducerStateChecker,
      Executor executor) {

      ......
      this.taskInfo = new TaskInfo(
            taskInformation.getTaskName(),
            taskInformation.getMaxNumberOfSubtaks(),
            subtaskIndex,
            taskInformation.getNumberOfSubtasks(),
            attemptNumber,
            String.valueOf(slotAllocationId));

      this.jobId = jobInformation.getJobId();
      this.vertexId = taskInformation.getJobVertexId();
      this.executionId  = Preconditions.checkNotNull(executionAttemptID);
      this.allocationId = Preconditions.checkNotNull(slotAllocationId);
      this.taskNameWithSubtask = taskInfo.getTaskNameWithSubtasks();
      this.jobConfiguration = jobInformation.getJobConfiguration();
      this.taskConfiguration = taskInformation.getTaskConfiguration();
      this.requiredJarFiles = jobInformation.getRequiredJarFileBlobKeys();
      this.requiredClasspaths = jobInformation.getRequiredClasspathURLs();
      this.nameOfInvokableClass = taskInformation.getInvokableClassName();
      this.serializedExecutionConfig = jobInformation.getSerializedExecutionConfig();

      Configuration tmConfig = taskManagerConfig.getConfiguration();
      this.taskCancellationInterval = tmConfig.getLong(TaskManagerOptions.TASK_CANCELLATION_INTERVAL);
      this.taskCancellationTimeout = tmConfig.getLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT);

      this.memoryManager = Preconditions.checkNotNull(memManager);
      this.ioManager = Preconditions.checkNotNull(ioManager);
      this.broadcastVariableManager = Preconditions.checkNotNull(bcVarManager);
      this.taskEventDispatcher = Preconditions.checkNotNull(taskEventDispatcher);
      this.taskStateManager = Preconditions.checkNotNull(taskStateManager);
      this.accumulatorRegistry = new AccumulatorRegistry(jobId, executionId);

      this.inputSplitProvider = Preconditions.checkNotNull(inputSplitProvider);
      this.checkpointResponder = Preconditions.checkNotNull(checkpointResponder);
      this.aggregateManager = Preconditions.checkNotNull(aggregateManager);
      this.taskManagerActions = checkNotNull(taskManagerActions);

      this.blobService = Preconditions.checkNotNull(blobService);
      this.libraryCache = Preconditions.checkNotNull(libraryCache);
      this.fileCache = Preconditions.checkNotNull(fileCache);
      this.kvStateService = Preconditions.checkNotNull(kvStateService);
      this.taskManagerConfig = Preconditions.checkNotNull(taskManagerConfig);

      this.metrics = metricGroup;

      this.partitionProducerStateChecker = Preconditions.checkNotNull(partitionProducerStateChecker);
      this.executor = Preconditions.checkNotNull(executor);

      // create the reader and writer structures

      final String taskNameWithSubtaskAndId = taskNameWithSubtask + " (" + executionId + ')';

      final ShuffleIOOwnerContext taskShuffleContext = shuffleEnvironment
         .createShuffleIOOwnerContext(taskNameWithSubtaskAndId, executionId, metrics.getIOMetricGroup());

      // produced intermediate result partitions
      //创建ResultPartition
      final ResultPartitionWriter[] resultPartitionWriters = shuffleEnvironment.createResultPartitionWriters(
         taskShuffleContext,
         resultPartitionDeploymentDescriptors).toArray(new ResultPartitionWriter[] {});

      this.consumableNotifyingPartitionWriters = ConsumableNotifyingResultPartitionWriterDecorator.decorate(
         resultPartitionDeploymentDescriptors,
         resultPartitionWriters,
         this,
         jobId,
         resultPartitionConsumableNotifier);

      // consumed intermediate result partitions
      //创建InputGate
      final InputGate[] gates = shuffleEnvironment.createInputGates(
         taskShuffleContext,
         this,
         inputGateDeploymentDescriptors).toArray(new InputGate[] {});

      this.inputGates = new InputGate[gates.length];
      int counter = 0;
      for (InputGate gate : gates) {
         inputGates[counter++] = new InputGateWithMetrics(gate, metrics.getIOMetricGroup().getNumBytesInCounter());
      }

      if (shuffleEnvironment instanceof NettyShuffleEnvironment) {
         //noinspection deprecation
         ((NettyShuffleEnvironment) shuffleEnvironment)
            .registerLegacyNetworkMetrics(metrics.getIOMetricGroup(), resultPartitionWriters, gates);
      }

      invokableHasBeenCanceled = new AtomicBoolean(false);

      // finally, create the executing thread, but do not start it
      executingThread = new Thread(TASK_THREADS_GROUP, this, taskNameWithSubtask);
   }
```

TaskSlotTable的addTask添加Task

```
public boolean addTask(T task) throws SlotNotFoundException, SlotNotActiveException {
   checkRunning();
   Preconditions.checkNotNull(task);

//获取该Task的slot信息
   TaskSlot<T> taskSlot = getTaskSlot(task.getAllocationId());

   if (taskSlot != null) {
      if (taskSlot.isActive(task.getJobID(), task.getAllocationId())) {
         if (taskSlot.add(task)) {
         //添加task和slot的映射
            taskSlotMappings.put(task.getExecutionId(), new TaskSlotMapping<>(task, taskSlot));

            return true;
         } else {
            return false;
         }
      } else {
         throw new SlotNotActiveException(task.getJobID(), task.getAllocationId());
      }
   } else {
      throw new SlotNotFoundException(task.getAllocationId());
   }
}
```

task.startTaskThread()

```
public void startTaskThread() {
		executingThread.start();
	}

public void run() {
   try {
      doRun();
   } finally {
      terminationFuture.complete(executionState);
   }
}

private void doRun() {
   // ----------------------------
   //  Initial State transition
   // ----------------------------
   while (true) {
      ExecutionState current = this.executionState;
      if (current == ExecutionState.CREATED) {
      //把CREATED状态转为DEPLOYING
         if (transitionState(ExecutionState.CREATED, ExecutionState.DEPLOYING)) {
            // success, we can start our work
            break;
         }
      }
      else if (current == ExecutionState.FAILED) {
         // we were immediately failed. tell the TaskManager that we reached our final state
         notifyFinalState();
         if (metrics != null) {
            metrics.close();
         }
         return;
      }
      else if (current == ExecutionState.CANCELING) {
         if (transitionState(ExecutionState.CANCELING, ExecutionState.CANCELED)) {
            // we were immediately canceled. tell the TaskManager that we reached our final state
            notifyFinalState();
            if (metrics != null) {
               metrics.close();
            }
            return;
         }
      }
      else {
         if (metrics != null) {
            metrics.close();
         }
         throw new IllegalStateException("Invalid state for beginning of operation of task " + this + '.');
      }
   }

   // all resource acquisitions and registrations from here on
   // need to be undone in the end
   Map<String, Future<Path>> distributedCacheEntries = new HashMap<>();
   AbstractInvokable invokable = null;

   try {
      // ----------------------------
      //  Task Bootstrap - We periodically
      //  check for canceling as a shortcut
      // ----------------------------

      // activate safety net for task thread
      LOG.info("Creating FileSystem stream leak safety net for task {}", this);
      FileSystemSafetyNet.initializeSafetyNetForThread();

      blobService.getPermanentBlobService().registerJob(jobId);

      // first of all, get a user-code classloader
      // this may involve downloading the job's JAR files and/or classes
      LOG.info("Loading JAR files for task {}.", this);

      userCodeClassLoader = createUserCodeClassloader();
      final ExecutionConfig executionConfig = serializedExecutionConfig.deserializeValue(userCodeClassLoader);

      if (executionConfig.getTaskCancellationInterval() >= 0) {
         // override task cancellation interval from Flink config if set in ExecutionConfig
         taskCancellationInterval = executionConfig.getTaskCancellationInterval();
      }

      if (executionConfig.getTaskCancellationTimeout() >= 0) {
         // override task cancellation timeout from Flink config if set in ExecutionConfig
         taskCancellationTimeout = executionConfig.getTaskCancellationTimeout();
      }

      if (isCanceledOrFailed()) {
         throw new CancelTaskException();
      }

      // ----------------------------------------------------------------
      // register the task with the network stack
      // this operation may fail if the system does not have enough
      // memory to run the necessary data exchanges
      // the registration must also strictly be undone
      // ----------------------------------------------------------------

      LOG.info("Registering task at network: {}.", this);

//初始化ResultPartition和InputGate，注册BufferPool
      setupPartitionsAndGates(consumableNotifyingPartitionWriters, inputGates);

//将ResultPartition注册到EventDispatcher，添加接收消息事件的监听
      for (ResultPartitionWriter partitionWriter : consumableNotifyingPartitionWriters) {
         taskEventDispatcher.registerPartition(partitionWriter.getPartitionId());
      }

//将文件写入分布式缓存
      // next, kick off the background copying of files for the distributed cache
      try {
         for (Map.Entry<String, DistributedCache.DistributedCacheEntry> entry :
               DistributedCache.readFileInfoFromConfig(jobConfiguration)) {
            LOG.info("Obtaining local cache file for '{}'.", entry.getKey());
            Future<Path> cp = fileCache.createTmpFile(entry.getKey(), entry.getValue(), jobId, executionId);
            distributedCacheEntries.put(entry.getKey(), cp);
         }
      }
      catch (Exception e) {
         throw new Exception(
            String.format("Exception while adding files to distributed cache of task %s (%s).", taskNameWithSubtask, executionId), e);
      }

      if (isCanceledOrFailed()) {
         throw new CancelTaskException();
      }

      // ----------------------------------------------------------------
      //  call the user code initialization methods
      // ----------------------------------------------------------------

      TaskKvStateRegistry kvStateRegistry = kvStateService.createKvStateTaskRegistry(jobId, getJobVertexId());

      Environment env = new RuntimeEnvironment(
         jobId,
         vertexId,
         executionId,
         executionConfig,
         taskInfo,
         jobConfiguration,
         taskConfiguration,
         userCodeClassLoader,
         memoryManager,
         ioManager,
         broadcastVariableManager,
         taskStateManager,
         aggregateManager,
         accumulatorRegistry,
         kvStateRegistry,
         inputSplitProvider,
         distributedCacheEntries,
         consumableNotifyingPartitionWriters,
         inputGates,
         taskEventDispatcher,
         checkpointResponder,
         taskManagerConfig,
         metrics,
         this);

      // Make sure the user code classloader is accessible thread-locally.
      // We are setting the correct context class loader before instantiating the invokable
      // so that it is available to the invokable during its entire lifetime.
      executingThread.setContextClassLoader(userCodeClassLoader);

// 每一个 StreamNode 在添加的时候都会有一个 jobVertexClass 属性
			// 对于一个 operator chain，就是 head operator 对应的 invokableClassName，见 StreamingJobGraphGenerator.createChain
			// 通过反射创建 AbstractInvokable 对象
			// 对于 Stream 任务而言，就是 StreamTask 的子类，SourceStreamTask、OneInputStreamTask、TwoInputStreamTask 等
      // now load and instantiate the task's invokable code
      invokable = loadAndInstantiateInvokable(userCodeClassLoader, nameOfInvokableClass, env);

      // ----------------------------------------------------------------
      //  actual task core work
      // ----------------------------------------------------------------

      // we must make strictly sure that the invokable is accessible to the cancel() call
      // by the time we switched to running.
      this.invokable = invokable;

      // switch to the RUNNING state, if that fails, we have been canceled/failed in the meantime
      if (!transitionState(ExecutionState.DEPLOYING, ExecutionState.RUNNING)) {
         throw new CancelTaskException();
      }

      // notify everyone that we switched to running
      taskManagerActions.updateTaskExecutionState(new TaskExecutionState(jobId, executionId, ExecutionState.RUNNING));

      // make sure the user code classloader is accessible thread-locally
      executingThread.setContextClassLoader(userCodeClassLoader);

      // run the invokable
      //运行StreamTask
      invokable.invoke();

      // make sure, we enter the catch block if the task leaves the invoke() method due
      // to the fact that it has been canceled
      if (isCanceledOrFailed()) {
         throw new CancelTaskException();
      }

      // ----------------------------------------------------------------
      //  finalization of a successful execution
      // ----------------------------------------------------------------

      // finish the produced partitions. if this fails, we consider the execution failed.
      for (ResultPartitionWriter partitionWriter : consumableNotifyingPartitionWriters) {
         if (partitionWriter != null) {
            partitionWriter.finish();
         }
      }

      // try to mark the task as finished
      // if that fails, the task was canceled/failed in the meantime
      if (!transitionState(ExecutionState.RUNNING, ExecutionState.FINISHED)) {
         throw new CancelTaskException();
      }
   }
   catch (Throwable t) {
......
      }
      }
```

AbstractInvokable的invoke调用StreamTask的invoke，`StreamTask` 完整的生命周期包括：

- 创建状态存储后端，为 OperatorChain 中的所有算子提供状态
- 加载 OperatorChain 中的所有算子
- 所有的 operator 调用 `setup`
- task 相关的初始化操作
- 所有 operator 调用 `initializeState` 初始化状态
- 所有的 operator 调用 `open`
- runMailboxLoop 方法循环处理数据
- 所有 operator 调用 `close`
- 所有 operator 调用 `dispose`
- 通用的 cleanup 操作
- task 相关的 cleanup 操作

```
public final void invoke() throws Exception {
   try {
      beforeInvoke();

      // final check to exit early before starting to run
      if (canceled) {
         throw new CancelTaskException();
      }

      // let the task do its work
      isRunning = true;
      runMailboxLoop();

      // if this left the run() method cleanly despite the fact that this was canceled,
      // make sure the "clean shutdown" is not attempted
      if (canceled) {
         throw new CancelTaskException();
      }

      afterInvoke();
   }
   finally {
      cleanUpInvoke();
   }
}
```

beforeInvoke

```
private void beforeInvoke() throws Exception {
   disposedOperators = false;
   LOG.debug("Initializing {}.", getName());

//准备异步线程池
   asyncOperationsThreadPool = Executors.newCachedThreadPool(new ExecutorThreadFactory("AsyncOperations", uncaughtExceptionHandler));

//创建状态存储后端
   stateBackend = createStateBackend();
   checkpointStorage = stateBackend.createCheckpointStorage(getEnvironment().getJobID());

   // if the clock is not already set, then assign a default TimeServiceProvider
   if (timerService == null) {
      ThreadFactory timerThreadFactory =
         new DispatcherThreadFactory(TRIGGER_THREAD_GROUP, "Time Trigger for " + getName());

      timerService = new SystemProcessingTimeService(
         this::handleTimerException,
         timerThreadFactory);
   }

//创建 OperatorChain，会加载每一个 operator，并调用 setup 方法
   operatorChain = new OperatorChain<>(this, recordWriter);
   headOperator = operatorChain.getHeadOperator();

   // task specific initialization
   // 由具体 StreamTask 子类实现初始化操作
   init();

   // save the work of reloading state, etc, if the task is already canceled
   if (canceled) {
      throw new CancelTaskException();
   }

   // -------- Invoke --------
   LOG.debug("Invoking {}", getName());

   // we need to make sure that any triggers scheduled in open() cannot be
   // executed before all operators are opened
   actionExecutor.runThrowing(() -> {
      // both the following operations are protected by the lock
      // so that we avoid race conditions in the case that initializeState()
      // registers a timer, that fires before the open() is called.

      initializeStateAndOpen();
   });
}
```

```
private void initializeStateAndOpen() throws Exception {

   StreamOperator<?>[] allOperators = operatorChain.getAllOperators();
//初始化operatorChain的所有StreamOperator状态并调用open方法
   for (StreamOperator<?> operator : allOperators) {
      if (null != operator) {
         operator.initializeState();
         operator.open();
      }
   }
}
```

beforeInvoke初始化之后进入task主流程runMailboxLoop()

```
private void runMailboxLoop() throws Exception {
   try {
   //进入 mailboxProcessor
      mailboxProcessor.runMailboxLoop();
   }
   catch (Exception e) {
      Optional<InterruptedException> interruption = ExceptionUtils.findThrowable(e, InterruptedException.class);
      if (interruption.isPresent()) {
         if (!canceled) {
            Thread.currentThread().interrupt();
            throw interruption.get();
         }
      } else if (canceled) {
         LOG.warn("Error while canceling task.", e);
      }
      else {
         throw e;
      }
   }
}
```

MailboxProcessor类,Mailbox的结构用来保证单线程执行操作

```
public void runMailboxLoop() throws Exception {

   final TaskMailbox localMailbox = mailbox;

// 检查当前运行线程是否是 mailbox 线程，只有 mailbox 线程能运行该方法
   Preconditions.checkState(
      localMailbox.isMailboxThread(),
      "Method must be executed by declared mailbox thread!");

   assert localMailbox.getState() == TaskMailbox.State.OPEN : "Mailbox must be opened!";

   final MailboxController defaultActionContext = new MailboxController(this);

// 如果有 mail 需要处理，这里会进行相应的处理，处理完才会进行下面的 event processing，进行 task 的 default action，也就是调用 processInput()
   while (processMail(localMailbox)) {
      mailboxDefaultAction.runDefaultAction(defaultActionContext); // lock is acquired inside default action as needed
   }
}
```

processMail会检测 MailBox中是否有 mail 需要处理，如果有的话，就做相应的处理，**一直将全部的 mail 处理完才会返回**，只要 loop 还在进行，这里就会返回 true，否则会返回 false。

```
private boolean processMail(TaskMailbox mailbox) throws Exception {

   // Doing this check is an optimization to only have a volatile read in the expected hot path, locks are only
   // acquired after this point.
   //taskmailbox 会将 queue 中的消息移到 batch，然后从 batch queue 中依次 take；新 mail 写入 queue。从 batch take 时避免加锁
   if (!mailbox.createBatch()) {
      // We can also directly return true because all changes to #isMailboxLoopRunning must be connected to
      // mailbox.hasMail() == true.
      // 消息为空时直接返回
      return true;
   }

   // Take mails in a non-blockingly and execute them.
   Optional<Mail> maybeMail;
   // 从 batch 获取 mail 执行，直到 batch 中的 mail 处理完
   while (isMailboxLoopRunning() && (maybeMail = mailbox.tryTakeFromBatch()).isPresent()) {
      maybeMail.get().run();
   }

   // If the default action is currently not available, we can run a blocking mailbox execution until the default
   // action becomes available again.
   //做一个状态检查，等待mail状态变为available
   while (isDefaultActionUnavailable() && isMailboxLoopRunning()) {
      mailbox.take(MIN_PRIORITY).run();
   }

   return isMailboxLoopRunning();
}
```

处理完Mailbox，进入MailboxDefaultAction类的runDefaultAction，最终进入StreamTask的processInput

```
this.mailboxProcessor = new MailboxProcessor(this::processInput, mailbox, actionExecutor);
```

对于 StreamTask 来说，event-processing 是在 processInput() 方法中实现的，调用StreamInputProcessor的processInput处理数据，StreamInputProcessor是StreamTask的成员变量，子类有StreamOneInputProcessor和StreamTwoInputProcessor，分别对应OneInputStreamTask和TwoInputStreamTask的成员变量

```
protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
// event 处理
   InputStatus status = inputProcessor.processInput();
   // 如果输入还有数据，并且 recordWriter 是可用的(之前的异步操作已经处理完成)，这里就直接返回了
   if (status == InputStatus.MORE_AVAILABLE && recordWriter.isAvailable()) {
      return;
   }
   if (status == InputStatus.END_OF_INPUT) {
   // 输入已经处理完了
      controller.allActionsCompleted();
      return;
   }
   // 代码进行到这里说明 input 或 output 没有准备好（比如当前流中没有数据）
   CompletableFuture<?> jointFuture = getInputOutputJointFuture(status);
   // 告诉 MailBox 先暂停 loop
   MailboxDefaultAction.Suspension suspendedDefaultAction = controller.suspendDefaultAction();
   // 等待 future 完成后，继续 mailbox loop（等待 input 和 output 可用后，才会继续）
   jointFuture.thenRun(suspendedDefaultAction::resume);
}
```

```
public interface StreamInputProcessor extends AvailabilityProvider, Closeable {
   /**
    * @return input status to estimate whether more records can be processed immediately or not.
    * If there are no more records available at the moment and the caller should check finished
    * state and/or {@link #getAvailableFuture()}.
    */
   InputStatus processInput() throws Exception;
}
```

以OneInputStreamTask看下StreamOneInputProcessor的处理过程,在调用的时候init会初始化StreamOneInputProcessor

```
public class OneInputStreamTask<IN, OUT> extends StreamTask{
@Override
	public void init() throws Exception {
		StreamConfig configuration = getConfiguration();
		int numberOfInputs = configuration.getNumberOfInputs();

		if (numberOfInputs > 0) {
			CheckpointedInputGate inputGate = createCheckpointedInputGate();
			TaskIOMetricGroup taskIOMetricGroup = getEnvironment().getMetricGroup().getIOMetricGroup();
			taskIOMetricGroup.gauge("checkpointAlignmentTime", inputGate::getAlignmentDurationNanos);

			DataOutput<IN> output = createDataOutput();
			StreamTaskInput<IN> input = createTaskInput(inputGate, output);
			//初始化StreamOneInputProcessor
			inputProcessor = new StreamOneInputProcessor<>(
				input,
				output,
				getCheckpointLock(),
				operatorChain);
		}
		headOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, this.inputWatermarkGauge);
		// wrap watermark gauge since registered metrics must be unique
		getEnvironment().getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, this.inputWatermarkGauge::getValue);
	}
}
```

随后在处理数据时调用StreamOneInputProcessor的processInput

```
@Override
public InputStatus processInput() throws Exception {
   InputStatus status = input.emitNext(output);

   if (status == InputStatus.END_OF_INPUT) {
      synchronized (lock) {
         operatorChain.endHeadOperatorInput(1);
      }
   }

   return status;
}
```

调用StreamTaskNetworkInput的emitNext

```
public InputStatus emitNext(DataOutput<T> output) throws Exception {

   while (true) {
      // get the stream element from the deserializer
      //反序列化
      if (currentRecordDeserializer != null) {
         DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
         if (result.isBufferConsumed()) {
         //如果buffer里面的数据已经被消费了，则归还buffer
            currentRecordDeserializer.getCurrentBuffer().recycleBuffer();
            currentRecordDeserializer = null;
         }

         if (result.isFullRecord()) {
         //得到了一条完整的记录
            processElement(deserializationDelegate.getInstance(), output);
            // 处理完一条数据
            return InputStatus.MORE_AVAILABLE;
         }
      }

      //获取下一个 BufferOrEvent，这是个阻塞的调用
      Optional<BufferOrEvent> bufferOrEvent = checkpointedInputGate.pollNext();
      if (bufferOrEvent.isPresent()) {
         processBufferOrEvent(bufferOrEvent.get());
      } else {
         if (checkpointedInputGate.isFinished()) {
            checkState(checkpointedInputGate.getAvailableFuture().isDone(), "Finished BarrierHandler should be available");
            if (!checkpointedInputGate.isEmpty()) {
               throw new IllegalStateException("Trailing data in checkpoint barrier handler.");
            }
            //上游已结束
            return InputStatus.END_OF_INPUT;
         }
         return InputStatus.NOTHING_AVAILABLE;
      }
   }
}
```

processElement

```
private void processElement(StreamElement recordOrMark, DataOutput<T> output) throws Exception {
   if (recordOrMark.isRecord()){
   //是一条正常的记录，调用 operator 的处理方法，最终会调用用户自定义的函数的处理方法
      output.emitRecord(recordOrMark.asRecord());
   } else if (recordOrMark.isWatermark()) {
      statusWatermarkValve.inputWatermark(recordOrMark.asWatermark(), lastChannel);
   } else if (recordOrMark.isLatencyMarker()) {
      output.emitLatencyMarker(recordOrMark.asLatencyMarker());
   } else if (recordOrMark.isStreamStatus()) {
      statusWatermarkValve.inputStreamStatus(recordOrMark.asStreamStatus(), lastChannel);
   } else {
      throw new UnsupportedOperationException("Unknown type of StreamElement");
   }
}
```

processBufferOrEvent

```
private void processBufferOrEvent(BufferOrEvent bufferOrEvent) throws IOException {
   if (bufferOrEvent.isBuffer()) {
   //如果是Buffer，要确定是哪个 channel 的，然后用对应 channel 的反序列化器解析
					//不同channel在反序列化的时候不能混淆
      lastChannel = bufferOrEvent.getChannelIndex();
      checkState(lastChannel != StreamTaskInput.UNSPECIFIED);
      currentRecordDeserializer = recordDeserializers[lastChannel];
      checkState(currentRecordDeserializer != null,
         "currentRecordDeserializer has already been released");

      currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
   }
   else {
      // Event received
      final AbstractEvent event = bufferOrEvent.getEvent();
      // TODO: with checkpointedInputGate.isFinished() we might not need to support any events on this level.
      if (event.getClass() != EndOfPartitionEvent.class) {
         throw new IOException("Unexpected event: " + event);
      }

      // release the record deserializer immediately,
      // which is very valuable in case of bounded stream
      releaseDeserializer(bufferOrEvent.getChannelIndex());
   }
}
```

进入StreamTaskNetworkOutput的emitRecord

```
public void emitRecord(StreamRecord<IN> record) throws Exception {
   synchronized (lock) {
      numRecordsIn.inc();
      operator.setKeyContextElement1(record);
      operator.processElement(record);
   }
}
```

调用 operator 的处理方法，最终会调用用户自定义的函数的处理方法

```
public void processElement(StreamRecord<IN> element) throws Exception {
   output.collect(element.replace(userFunction.map(element.getValue())));
}
```

SourceStreamTask的实现稍有不同，主要是因为有一个问题：就是 *SourceStreamTask*，会有一个兼容性的问题，因为在流的 source 端，它的 event prcessing 是来专门产生一个无限流数据，在这个处理中，并不能穿插 MailBox 中的 mail 检测，也就是说，如果只有一个 MailBox 线程处理的话，当这个线程去产生数据的话，它一直运行下去，就无法再去检测 MailBox 中是否有新的 mail 到来（在 Source 未来的版本中，可以完美兼容 MailBox 线程设计，见 [FLIP-27](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface)，但现在的版本还不兼容）。

为了兼容 Source 端，目前的解决方案是：**两个线程操作，一个专门用产生无限流，另一个是 MailBox 线程（处理 Checkpoint、timer 等），这两个线程为了保证线程安全，还是使用 Checkpoint Lock 做排它锁**

```
protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {

// 告诉 MailBox 先暂停 loop
   controller.suspendDefaultAction();

   // Against the usual contract of this method, this implementation is not step-wise but blocking instead for
   // compatibility reasons with the current source interface (source functions run as a loop, not in steps).
   sourceThread.setTaskDescription(getName());
   sourceThread.start();
   sourceThread.getCompletionFuture().whenComplete((Void ignore, Throwable sourceThreadThrowable) -> {
      if (sourceThreadThrowable == null || isFinished) {
      // sourceThread 完成后，没有抛出异常或 task 完成的情况下
         mailboxProcessor.allActionsCompleted();
      } else {
      // 没有完成或者抛出异常的情况下
         mailboxProcessor.reportThrowable(sourceThreadThrowable);
      }
   });
}


private class LegacySourceFunctionThread extends Thread {

		private final CompletableFuture<Void> completionFuture;

		LegacySourceFunctionThread() {
			this.completionFuture = new CompletableFuture<>();
		}

		@Override
		public void run() {
			try {
				headOperator.run(getCheckpointLock(), getStreamStatusMaintainer(), operatorChain);
				completionFuture.complete(null);
			} catch (Throwable t) {
				// Note, t can be also an InterruptedException
				completionFuture.completeExceptionally(t);
			}
		}

		public void setTaskDescription(final String taskDescription) {
			setName("Legacy Source Thread - " + taskDescription);
		}

		CompletableFuture<Void> getCompletionFuture() {
			return completionFuture;
		}
	}
```

可以看到：

1.*LegacySourceFunctionThread* 线程在启动时，会先通知一下 MailBox，这个就是上面说的那个状态检查，收到这个信号之后，MailBox 就会在 processMail() 中一直等待并且处理 mail，不会返回（也就是 MailBox 主线程一直在处理 mail 事件）；

2.*LegacySourceFunctionThread* 线程就是专门生产数据的，跟 MailBox 这两个线程都在运行。

那么两个线程如何保证线程安全呢？如果仔细看上面的代码就会发现，在 SourceStreamTask 中还继续使用了 getCheckpointLock()，虽然这个方法现在已经被标注了将要被废弃，但 Source 没有改造完成之前，Source 的实现还是会继续依赖 checkpoint lock。

数据处理完进入afterInvoke，所有 operator 调用 close，所有 operator 调用 dispose

```
private void afterInvoke() throws Exception {
   LOG.debug("Finished task {}", getName());

   // make sure no further checkpoint and notification actions happen.
   // we make sure that no other thread is currently in the locked scope before
   // we close the operators by trying to acquire the checkpoint scope lock
   // we also need to make sure that no triggers fire concurrently with the close logic
   // at the same time, this makes sure that during any "regular" exit where still
   actionExecutor.runThrowing(() -> {
      // this is part of the main logic, so if this fails, the task is considered failed
      closeAllOperators();

      // make sure no new timers can come
      timerService.quiesce();

      // let mailbox execution reject all new letters from this point
      mailboxProcessor.prepareClose();

      // only set the StreamTask to not running after all operators have been closed!
      // See FLINK-7430
      isRunning = false;
   });
   // processes the remaining mails; no new mails can be enqueued
   mailboxProcessor.drain();

   // make sure all timers finish
   timerService.awaitPendingAfterQuiesce();

   LOG.debug("Closed operators for task {}", getName());

   // make sure all buffered data is flushed
   operatorChain.flushOutputs();

   // make an attempt to dispose the operators such that failures in the dispose call
   // still let the computation fail
   disposeAllOperators(false);
   disposedOperators = true;
}
```

看下beforeinvoke中OperatorChain的初始化过程

```
public OperatorChain(
      StreamTask<OUT, OP> containingTask,
      RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>> recordWriterDelegate) {

   final ClassLoader userCodeClassloader = containingTask.getUserCodeClassLoader();
   final StreamConfig configuration = containingTask.getConfiguration();

   StreamOperatorFactory<OUT> operatorFactory = configuration.getStreamOperatorFactory(userCodeClassloader);

   // we read the chained configs, and the order of record writer registrations by output name
   //OperatorChain 内部所有的 operator 的配置
   Map<Integer, StreamConfig> chainedConfigs = configuration.getTransitiveChainedTaskConfigsWithSelf(userCodeClassloader);

   // create the final output stream writers
   // we iterate through all the out edges from this job vertex and create a stream output
   // 所有的输出边，这是对外输出，不包含内部 operator 之间的的数据传输
   List<StreamEdge> outEdgesInOrder = configuration.getOutEdgesInOrder(userCodeClassloader);
   Map<StreamEdge, RecordWriterOutput<?>> streamOutputMap = new HashMap<>(outEdgesInOrder.size());
   this.streamOutputs = new RecordWriterOutput<?>[outEdgesInOrder.size()];

   // from here on, we need to make sure that the output writers are shut down again on failure
   boolean success = false;
   try {
   //创建对外输出的 RecordWriterOutput
      for (int i = 0; i < outEdgesInOrder.size(); i++) {
         StreamEdge outEdge = outEdgesInOrder.get(i);

         RecordWriterOutput<?> streamOutput = createStreamOutput(
            recordWriterDelegate.getRecordWriter(i),
            outEdge,
            chainedConfigs.get(outEdge.getSourceId()),
            containingTask.getEnvironment());

         this.streamOutputs[i] = streamOutput;
         streamOutputMap.put(outEdge, streamOutput);
      }

      // we create the chain of operators and grab the collector that leads into the chain
      //这里会递归调用，为 OperatorChain 内部的所有的 Operator 都创建 output
      List<StreamOperator<?>> allOps = new ArrayList<>(chainedConfigs.size());
      this.chainEntryPoint = createOutputCollector(
         containingTask,
         configuration,
         chainedConfigs,
         userCodeClassloader,
         streamOutputMap,
         allOps,
         containingTask.getMailboxExecutorFactory());

      if (operatorFactory != null) {
      //chainEntryPoint 是 headOperator 的 output
         WatermarkGaugeExposingOutput<StreamRecord<OUT>> output = getChainEntryPoint();

     //创建headOperator
         headOperator = StreamOperatorFactoryUtil.createOperator(
               operatorFactory,
               containingTask,
               configuration,
               output);

         headOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_OUTPUT_WATERMARK, output.getWatermarkGauge());
      } else {
         headOperator = null;
      }

      // add head operator to end of chain
      allOps.add(headOperator);

      this.allOperators = allOps.toArray(new StreamOperator<?>[allOps.size()]);

      success = true;
   }
   finally {
      // make sure we clean up after ourselves in case of a failure after acquiring
      // the first resources
      if (!success) {
         for (RecordWriterOutput<?> output : this.streamOutputs) {
            if (output != null) {
               output.close();
            }
         }
      }
   }
}
```

WatermarkAssignerOperatorFactory类的createStreamOperator

```
public StreamOperator createStreamOperator(StreamTask containingTask, StreamConfig config, Output output) {
   WatermarkGenerator watermarkGenerator = generatedWatermarkGenerator.newInstance(containingTask.getUserCodeClassLoader());
   WatermarkAssignerOperator operator = new WatermarkAssignerOperator(rowtimeFieldIndex, watermarkGenerator, idleTimeout);
   //调用headoperator的setup
   operator.setup(containingTask, config, output);
   return operator;
}
```

createStreamOutput

```
private RecordWriterOutput<OUT> createStreamOutput(
      RecordWriter<SerializationDelegate<StreamRecord<OUT>>> recordWriter,
      StreamEdge edge,
      StreamConfig upStreamConfig,
      Environment taskEnvironment) {
   OutputTag sideOutputTag = edge.getOutputTag(); // OutputTag, return null if not sideOutput

   TypeSerializer outSerializer = null;

   if (edge.getOutputTag() != null) {
      // side output
      //拿到side output的反序列化器
      outSerializer = upStreamConfig.getTypeSerializerSideOut(
            edge.getOutputTag(), taskEnvironment.getUserClassLoader());
   } else {
      // main output
      //拿到main output的反序列化器
      outSerializer = upStreamConfig.getTypeSerializerOut(taskEnvironment.getUserClassLoader());
   }

   return new RecordWriterOutput<>(recordWriter, outSerializer, sideOutputTag, this);
}
```

createOutputCollector

```
private <T> WatermarkGaugeExposingOutput<StreamRecord<T>> createOutputCollector(
      StreamTask<?, ?> containingTask,
      StreamConfig operatorConfig,
      Map<Integer, StreamConfig> chainedConfigs,
      ClassLoader userCodeClassloader,
      Map<StreamEdge, RecordWriterOutput<?>> streamOutputs,
      List<StreamOperator<?>> allOperators,
      MailboxExecutorFactory mailboxExecutorFactory) {
   List<Tuple2<WatermarkGaugeExposingOutput<StreamRecord<T>>, StreamEdge>> allOutputs = new ArrayList<>(4);

   // create collectors for the network outputs
   for (StreamEdge outputEdge : operatorConfig.getNonChainedOutputs(userCodeClassloader)) {
      @SuppressWarnings("unchecked")
      RecordWriterOutput<T> output = (RecordWriterOutput<T>) streamOutputs.get(outputEdge);

      allOutputs.add(new Tuple2<>(output, outputEdge));
   }

   // Create collectors for the chained outputs
   // OperatorChain 内部 Operator 之间的边
   for (StreamEdge outputEdge : operatorConfig.getChainedOutputs(userCodeClassloader)) {
      int outputId = outputEdge.getTargetId();
      StreamConfig chainedOpConfig = chainedConfigs.get(outputId);

      //创建当前节点的下游节点，并返回当前节点的 output
	//createChainedOperator 在创建 operator 的时候，会调用 createOutputCollector 为 operator 创建 output
	//所以会形成递归调用关系，所有的 operator 以及它们的 output 都会被创建出来
      WatermarkGaugeExposingOutput<StreamRecord<T>> output = createChainedOperator(
         containingTask,
         chainedOpConfig,
         chainedConfigs,
         userCodeClassloader,
         streamOutputs,
         allOperators,
         outputEdge.getOutputTag(),
         mailboxExecutorFactory);
      allOutputs.add(new Tuple2<>(output, outputEdge));
   }

   // if there are multiple outputs, or the outputs are directed, we need to
   // wrap them as one output

   List<OutputSelector<T>> selectors = operatorConfig.getOutputSelectors(userCodeClassloader);

   if (selectors == null || selectors.isEmpty()) {
      // simple path, no selector necessary
      //只有一个输出
      if (allOutputs.size() == 1) {
         return allOutputs.get(0).f0;
      }
      else {
      //不止有一个输出，需要使用 BroadcastingOutputCollector 进行封装
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
     // 存在 selector，用 DirectedOutput 进行封装
      // This is the inverse of creating the normal ChainingOutput.
      // If the chaining output does not copy we need to copy in the broadcast output,
      // otherwise multi-chaining would not work correctly.
      if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
         return new CopyingDirectedOutput<>(selectors, allOutputs);
      } else {
         return new DirectedOutput<>(selectors, allOutputs);
      }

   }
}
```

createChainedOperator

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
   // 为当前 Operator 创建 output
   WatermarkGaugeExposingOutput<StreamRecord<OUT>> chainedOperatorOutput = createOutputCollector(
      containingTask,
      operatorConfig,
      chainedConfigs,
      userCodeClassloader,
      streamOutputs,
      allOperators,
      mailboxExecutorFactory);

   // now create the operator and give it the output collector to write its output to
   //从 StreamConfig 中取出当前 Operator并调用setup
   OneInputStreamOperator<IN, OUT> chainedOperator = StreamOperatorFactoryUtil.createOperator(
         operatorConfig.getStreamOperatorFactory(userCodeClassloader),
         containingTask,
         operatorConfig,
         chainedOperatorOutput);

   allOperators.add(chainedOperator);

  //这里是在为当前 operator 前向的 operator 创建 output
		//所以当前 operator 被传递给前一个 operator 的 output，这样前一个 operator 的输出就可以直接调用当前 operator
   WatermarkGaugeExposingOutput<StreamRecord<IN>> currentOperatorOutput;
   if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
      currentOperatorOutput = new ChainingOutput<>(chainedOperator, this, outputTag);
   }
   else {
      TypeSerializer<IN> inSerializer = operatorConfig.getTypeSerializerIn1(userCodeClassloader);
      currentOperatorOutput = new CopyingChainingOutput<>(chainedOperator, inSerializer, outputTag, this);
   }

   // wrap watermark gauges since registered metrics must be unique
   chainedOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, currentOperatorOutput.getWatermarkGauge()::getValue);
   chainedOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_OUTPUT_WATERMARK, chainedOperatorOutput.getWatermarkGauge()::getValue);

   return currentOperatorOutput;
}
```