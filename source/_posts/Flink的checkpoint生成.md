checkpoint的发起过程，首先在生产jobgraph的过程中会配置Checkpoint。

在StreamingJobGraphGenerator类中

```
private JobGraph createJobGraph() {
   ......

// 配置 checkpoint
   configureCheckpointing();

  ......

   return jobGraph;
}

//获取Checkpoint的所有配置
private void configureCheckpointing() {
		CheckpointConfig cfg = streamGraph.getCheckpointConfig();

		long interval = cfg.getCheckpointInterval();
		if (interval < MINIMAL_CHECKPOINT_TIME) {
			// interval of max value means disable periodic checkpoint
			interval = Long.MAX_VALUE;
		}

		//  --- configure the participating vertices ---

		// collect the vertices that receive "trigger checkpoint" messages.
		// currently, these are all the sources
		// 只包含那些作为 source 的节点
		List<JobVertexID> triggerVertices = new ArrayList<>();

		// collect the vertices that need to acknowledge the checkpoint
		// currently, these are all vertices
		List<JobVertexID> ackVertices = new ArrayList<>(jobVertices.size());

		// collect the vertices that receive "commit checkpoint" messages
		// currently, these are all vertices
		List<JobVertexID> commitVertices = new ArrayList<>(jobVertices.size());

		for (JobVertex vertex : jobVertices.values()) {
			if (vertex.isInputVertex()) {
				triggerVertices.add(vertex.getID());
			}
			commitVertices.add(vertex.getID());
			ackVertices.add(vertex.getID());
		}

		//  --- configure options ---

		CheckpointRetentionPolicy retentionAfterTermination;
		if (cfg.isExternalizedCheckpointsEnabled()) {
			CheckpointConfig.ExternalizedCheckpointCleanup cleanup = cfg.getExternalizedCheckpointCleanup();
			// Sanity check
			if (cleanup == null) {
				throw new IllegalStateException("Externalized checkpoints enabled, but no cleanup mode configured.");
			}
			retentionAfterTermination = cleanup.deleteOnCancellation() ?
					CheckpointRetentionPolicy.RETAIN_ON_FAILURE :
					CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION;
		} else {
			retentionAfterTermination = CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
		}

		CheckpointingMode mode = cfg.getCheckpointingMode();

		boolean isExactlyOnce;
		if (mode == CheckpointingMode.EXACTLY_ONCE) {
			isExactlyOnce = true;
		} else if (mode == CheckpointingMode.AT_LEAST_ONCE) {
			isExactlyOnce = false;
		} else {
			throw new IllegalStateException("Unexpected checkpointing mode. " +
				"Did not expect there to be another checkpointing mode besides " +
				"exactly-once or at-least-once.");
		}

		//  --- configure the master-side checkpoint hooks ---

		final ArrayList<MasterTriggerRestoreHook.Factory> hooks = new ArrayList<>();

		for (StreamNode node : streamGraph.getStreamNodes()) {
			if (node.getOperatorFactory() instanceof UdfStreamOperatorFactory) {
				Function f = ((UdfStreamOperatorFactory) node.getOperatorFactory()).getUserFunction();

				if (f instanceof WithMasterCheckpointHook) {
					hooks.add(new FunctionMasterCheckpointHookFactory((WithMasterCheckpointHook<?>) f));
				}
			}
		}

		// because the hooks can have user-defined code, they need to be stored as
		// eagerly serialized values
		final SerializedValue<MasterTriggerRestoreHook.Factory[]> serializedHooks;
		if (hooks.isEmpty()) {
			serializedHooks = null;
		} else {
			try {
				MasterTriggerRestoreHook.Factory[] asArray =
						hooks.toArray(new MasterTriggerRestoreHook.Factory[hooks.size()]);
				serializedHooks = new SerializedValue<>(asArray);
			}
			catch (IOException e) {
				throw new FlinkRuntimeException("Trigger/restore hook is not serializable", e);
			}
		}

		// because the state backend can have user-defined code, it needs to be stored as
		// eagerly serialized value
		final SerializedValue<StateBackend> serializedStateBackend;
		if (streamGraph.getStateBackend() == null) {
			serializedStateBackend = null;
		} else {
			try {
				serializedStateBackend =
					new SerializedValue<StateBackend>(streamGraph.getStateBackend());
			}
			catch (IOException e) {
				throw new FlinkRuntimeException("State backend is not serializable", e);
			}
		}

		//  --- done, put it all together ---
       //加入jobGraph的配置
		JobCheckpointingSettings settings = new JobCheckpointingSettings(
			triggerVertices,
			ackVertices,
			commitVertices,
			new CheckpointCoordinatorConfiguration(
				interval,
				cfg.getCheckpointTimeout(),
				cfg.getMinPauseBetweenCheckpoints(),
				cfg.getMaxConcurrentCheckpoints(),
				retentionAfterTermination,
				isExactlyOnce,
				cfg.isPreferCheckpointForRecovery(),
				cfg.getTolerableCheckpointFailureNumber()),
			serializedStateBackend,
			serializedHooks);

		jobGraph.setSnapshotSettings(settings);
	}
}
```

接着jobGraph进入ExecutionGraph的构建过程

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

   ......

//根据jobGraph的Checkpoint的配置来配置ExecutionGraph
   // configure the state checkpointing
   JobCheckpointingSettings snapshotSettings = jobGraph.getCheckpointingSettings();
   if (snapshotSettings != null) {
      List<ExecutionJobVertex> triggerVertices =
            idToVertex(snapshotSettings.getVerticesToTrigger(), executionGraph);

      List<ExecutionJobVertex> ackVertices =
            idToVertex(snapshotSettings.getVerticesToAcknowledge(), executionGraph);

      List<ExecutionJobVertex> confirmVertices =
            idToVertex(snapshotSettings.getVerticesToConfirm(), executionGraph);

      CompletedCheckpointStore completedCheckpoints;
      CheckpointIDCounter checkpointIdCounter;
      try {
         int maxNumberOfCheckpointsToRetain = jobManagerConfig.getInteger(
               CheckpointingOptions.MAX_RETAINED_CHECKPOINTS);

         if (maxNumberOfCheckpointsToRetain <= 0) {
            // warning and use 1 as the default value if the setting in
            // state.checkpoints.max-retained-checkpoints is not greater than 0.
            log.warn("The setting for '{} : {}' is invalid. Using default value of {}",
                  CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.key(),
                  maxNumberOfCheckpointsToRetain,
                  CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.defaultValue());

            maxNumberOfCheckpointsToRetain = CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.defaultValue();
         }

         completedCheckpoints = recoveryFactory.createCheckpointStore(jobId, maxNumberOfCheckpointsToRetain, classLoader);
         checkpointIdCounter = recoveryFactory.createCheckpointIDCounter(jobId);
      }
      catch (Exception e) {
         throw new JobExecutionException(jobId, "Failed to initialize high-availability checkpoint handler", e);
      }

      // Maximum number of remembered checkpoints
      
      int historySize = jobManagerConfig.getInteger(WebOptions.CHECKPOINTS_HISTORY_SIZE);

      CheckpointStatsTracker checkpointStatsTracker = new CheckpointStatsTracker(
            historySize,
            ackVertices,
            snapshotSettings.getCheckpointCoordinatorConfiguration(),
            metrics);

      // load the state backend from the application settings
      final StateBackend applicationConfiguredBackend;
      final SerializedValue<StateBackend> serializedAppConfigured = snapshotSettings.getDefaultStateBackend();

      if (serializedAppConfigured == null) {
         applicationConfiguredBackend = null;
      }
      else {
         try {
            applicationConfiguredBackend = serializedAppConfigured.deserializeValue(classLoader);
         } catch (IOException | ClassNotFoundException e) {
            throw new JobExecutionException(jobId,
                  "Could not deserialize application-defined state backend.", e);
         }
      }

      final StateBackend rootBackend;
      try {
         rootBackend = StateBackendLoader.fromApplicationOrConfigOrDefault(
               applicationConfiguredBackend, jobManagerConfig, classLoader, log);
      }
      catch (IllegalConfigurationException | IOException | DynamicCodeLoadingException e) {
         throw new JobExecutionException(jobId, "Could not instantiate configured state backend", e);
      }

      // instantiate the user-defined checkpoint hooks
//回调用户自己实现的Checkpoint逻辑
      final SerializedValue<MasterTriggerRestoreHook.Factory[]> serializedHooks = snapshotSettings.getMasterHooks();
      final List<MasterTriggerRestoreHook<?>> hooks;

      if (serializedHooks == null) {
         hooks = Collections.emptyList();
      }
      else {
         final MasterTriggerRestoreHook.Factory[] hookFactories;
         try {
            hookFactories = serializedHooks.deserializeValue(classLoader);
         }
         catch (IOException | ClassNotFoundException e) {
            throw new JobExecutionException(jobId, "Could not instantiate user-defined checkpoint hooks", e);
         }

         final Thread thread = Thread.currentThread();
         final ClassLoader originalClassLoader = thread.getContextClassLoader();
         thread.setContextClassLoader(classLoader);

         try {
            hooks = new ArrayList<>(hookFactories.length);
            for (MasterTriggerRestoreHook.Factory factory : hookFactories) {
               hooks.add(MasterHooks.wrapHook(factory.create(), classLoader));
            }
         }
         finally {
            thread.setContextClassLoader(originalClassLoader);
         }
      }

      final CheckpointCoordinatorConfiguration chkConfig = snapshotSettings.getCheckpointCoordinatorConfiguration();

//开启Checkpoint
      executionGraph.enableCheckpointing(
         chkConfig,
         triggerVertices,
         ackVertices,
         confirmVertices,
         hooks,
         checkpointIdCounter,
         completedCheckpoints,
         rootBackend,
         checkpointStatsTracker);
   }

   return executionGraph;
}
```

executionGraph.enableCheckpointing开启Checkpoint

```
public void enableCheckpointing(
      CheckpointCoordinatorConfiguration chkConfig,
      List<ExecutionJobVertex> verticesToTrigger,
      List<ExecutionJobVertex> verticesToWaitFor,
      List<ExecutionJobVertex> verticesToCommitTo,
      List<MasterTriggerRestoreHook<?>> masterHooks,
      CheckpointIDCounter checkpointIDCounter,
      CompletedCheckpointStore checkpointStore,
      StateBackend checkpointStateBackend,
      CheckpointStatsTracker statsTracker) {

   checkState(state == JobStatus.CREATED, "Job must be in CREATED state");
   checkState(checkpointCoordinator == null, "checkpointing already enabled");

   ExecutionVertex[] tasksToTrigger = collectExecutionVertices(verticesToTrigger);
   ExecutionVertex[] tasksToWaitFor = collectExecutionVertices(verticesToWaitFor);
   ExecutionVertex[] tasksToCommitTo = collectExecutionVertices(verticesToCommitTo);

   checkpointStatsTracker = checkNotNull(statsTracker, "CheckpointStatsTracker");

   CheckpointFailureManager failureManager = new CheckpointFailureManager(
      chkConfig.getTolerableCheckpointFailureNumber(),
      new CheckpointFailureManager.FailJobCallback() {
         @Override
         public void failJob(Throwable cause) {
            getJobMasterMainThreadExecutor().execute(() -> failGlobal(cause));
         }

         @Override
         public void failJobDueToTaskFailure(Throwable cause, ExecutionAttemptID failingTask) {
            getJobMasterMainThreadExecutor().execute(() -> failGlobalIfExecutionIsStillRunning(cause, failingTask));
         }
      }
   );

   checkState(checkpointCoordinatorTimer == null);

   checkpointCoordinatorTimer = Executors.newSingleThreadScheduledExecutor(
      new DispatcherThreadFactory(
         Thread.currentThread().getThreadGroup(), "Checkpoint Timer"));

   // create the coordinator that triggers and commits checkpoints and holds the state
   //创建 CheckpointCoordinator 对象
   checkpointCoordinator = new CheckpointCoordinator(
      jobInformation.getJobId(),
      chkConfig,
      tasksToTrigger,
      tasksToWaitFor,
      tasksToCommitTo,
      checkpointIDCounter,
      checkpointStore,
      checkpointStateBackend,
      ioExecutor,
      new ScheduledExecutorServiceAdapter(checkpointCoordinatorTimer),
      SharedStateRegistry.DEFAULT_FACTORY,
      failureManager);

   // register the master hooks on the checkpoint coordinator
   for (MasterTriggerRestoreHook<?> hook : masterHooks) {
      if (!checkpointCoordinator.addMasterHook(hook)) {
         LOG.warn("Trying to register multiple checkpoint hooks with the name: {}", hook.getIdentifier());
      }
   }

   checkpointCoordinator.setCheckpointStatsTracker(checkpointStatsTracker);

   // interval of max long value indicates disable periodic checkpoint,
   // the CheckpointActivatorDeactivator should be created only if the interval is not max value
   if (chkConfig.getCheckpointInterval() != Long.MAX_VALUE) {
      // the periodic checkpoint scheduler is activated and deactivated as a result of
      // job status changes (running -> on, all other states -> off)
      //注册一个作业状态的监听 CheckpointCoordinatorDeActivator
      registerJobStatusListener(checkpointCoordinator.createActivatorDeactivator());
   }

   this.stateBackendName = checkpointStateBackend.getClass().getSimpleName();
}
```

CheckpointCoordinatorDeActivator实现JobStatusListener接口，当Job状态变为 RUNNING 时，通过startCheckpointScheduler启动 checkpoint 的定时器

```
public class CheckpointCoordinatorDeActivator implements JobStatusListener {

   private final CheckpointCoordinator coordinator;

   public CheckpointCoordinatorDeActivator(CheckpointCoordinator coordinator) {
      this.coordinator = checkNotNull(coordinator);
   }

   @Override
   public void jobStatusChanges(JobID jobId, JobStatus newJobStatus, long timestamp, Throwable error) {
      if (newJobStatus == JobStatus.RUNNING) {
         // start the checkpoint scheduler
         coordinator.startCheckpointScheduler();
      } else {
         // anything else should stop the trigger for now
         coordinator.stopCheckpointScheduler();
      }
   }
}
```

CheckpointCoordinator类

```
public void startCheckpointScheduler() {
   synchronized (lock) {
      if (shutdown) {
         throw new IllegalArgumentException("Checkpoint coordinator is shut down");
      }

      // make sure all prior timers are cancelled
      stopCheckpointScheduler();

      periodicScheduling = true;
      currentPeriodicTrigger = scheduleTriggerWithDelay(getRandomInitDelay());
   }
}

//开启定时任务，baseInterval从配置文件getCheckpointInterval获取
private ScheduledFuture<?> scheduleTriggerWithDelay(long initDelay) {
		return timer.scheduleAtFixedRate(
			new ScheduledTrigger(),
			initDelay, baseInterval, TimeUnit.MILLISECONDS);
	}
```

定时任务的内容就是ScheduledTrigger类 的triggerCheckpoint，包括以下几个步骤：

- 检查是否可以触发 checkpoint，包括是否需要强制进行 checkpoint，当前正在排队的并发 checkpoint 的数目是否超过阈值，距离上一次成功 checkpoint 的间隔时间是否过小等，如果这些条件不满足，则当前检查点的触发请求不会执行
- 检查是否所有需要触发 checkpoint 的 Execution 都是 `RUNNING` 状态
- 生成此次 checkpoint 的 checkpointID（id 是严格自增的），并初始化 `CheckpointStorageLocation`，`CheckpointStorageLocation` 是此次 checkpoint 存储位置的抽象，通过 `CheckpointStorage.initializeLocationForCheckpoint()` 创建（`CheckpointStorage` 目前有两个具体实现，分别为 `FsCheckpointStorage` 和 `MemoryBackendCheckpointStorage`），`CheckpointStorage` 则是从 `StateBackend` 中创建
- 生成 `PendingCheckpoint`，这表示一个处于中间状态的 checkpoint，并保存在 `checkpointId -> PendingCheckpoint` 这样的映射关系中
- 注册一个调度任务，在 checkpoint 超时后取消此次 checkpoint，并重新触发一次新的 checkpoint
- 调用 `Execution.triggerCheckpoint()` 方法向所有需要 trigger 的 task 发起 checkpoint 请求

savepoint 和 checkpoint 的处理逻辑基本一致，只是 savepoint 是强制触发的，需要调用 `Execution.triggerSynchronousSavepoint()` 进行触发。

在CheckpointCoordinator 内部也有三个列表：

- `ExecutionVertex[] tasksToTrigger`;
- `ExecutionVertex[] tasksToWaitFor`;
- `ExecutionVertex[] tasksToCommitTo`;

这就对应了前面 `JobGraph` 中的三个列表，在触发 checkpoint 的时候，只有作为 source 的 Execution 会调用 `Execution.triggerCheckpoint()` 方法。会通过 RPC 调用通知对应的 `RpcTaskManagerGateway` 调用 `triggerCheckpoint`。：

```
private final class ScheduledTrigger implements Runnable {

   @Override
   public void run() {
      try {
         triggerCheckpoint(System.currentTimeMillis(), true);
      }
      catch (Exception e) {
         LOG.error("Exception while triggering checkpoint for job {}.", job, e);
      }
   }
}

public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(long timestamp, boolean isPeriodic) {
		try {
		//进入triggerCheckpoint
			return triggerCheckpoint(timestamp, checkpointProperties, null, isPeriodic, false);
		} catch (CheckpointException e) {
			long latestGeneratedCheckpointId = getCheckpointIdCounter().get();
			// here we can not get the failed pending checkpoint's id,
			// so we pass the negative latest generated checkpoint id as a special flag
			failureManager.handleJobLevelCheckpointException(e, -1 * latestGeneratedCheckpointId);
			return FutureUtils.completedExceptionally(e);
		}
	}


	@VisibleForTesting
	public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(
			long timestamp,
			CheckpointProperties props,
			@Nullable String externalSavepointLocation,
			boolean isPeriodic,
			boolean advanceToEndOfTime) throws CheckpointException {

		if (advanceToEndOfTime && !(props.isSynchronous() && props.isSavepoint())) {
			throw new IllegalArgumentException("Only synchronous savepoints are allowed to advance the watermark to MAX.");
		}

		// make some eager pre-checks
		synchronized (lock) {
		//检查是否可以触发 checkpoint
			preCheckBeforeTriggeringCheckpoint(isPeriodic, props.forceCheckpoint());
		}

		// check if all tasks that we need to trigger are running.
		// if not, abort the checkpoint
		//检查是否所有需要触发 checkpoint 的 Execution 都是 RUNNING 状态
		Execution[] executions = new Execution[tasksToTrigger.length];
		for (int i = 0; i < tasksToTrigger.length; i++) {
			Execution ee = tasksToTrigger[i].getCurrentExecutionAttempt();
			if (ee == null) {
				LOG.info("Checkpoint triggering task {} of job {} is not being executed at the moment. Aborting checkpoint.",
						tasksToTrigger[i].getTaskNameWithSubtaskIndex(),
						job);
				throw new CheckpointException(CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
			} else if (ee.getState() == ExecutionState.RUNNING) {
				executions[i] = ee;
			} else {
				LOG.info("Checkpoint triggering task {} of job {} is not in state {} but {} instead. Aborting checkpoint.",
						tasksToTrigger[i].getTaskNameWithSubtaskIndex(),
						job,
						ExecutionState.RUNNING,
						ee.getState());
				throw new CheckpointException(CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
			}
		}

		// next, check if all tasks that need to acknowledge the checkpoint are running.
		// if not, abort the checkpoint
		Map<ExecutionAttemptID, ExecutionVertex> ackTasks = new HashMap<>(tasksToWaitFor.length);

		for (ExecutionVertex ev : tasksToWaitFor) {
			Execution ee = ev.getCurrentExecutionAttempt();
			if (ee != null) {
				ackTasks.put(ee.getAttemptId(), ev);
			} else {
				LOG.info("Checkpoint acknowledging task {} of job {} is not being executed at the moment. Aborting checkpoint.",
						ev.getTaskNameWithSubtaskIndex(),
						job);
				throw new CheckpointException(CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
			}
		}

		// we will actually trigger this checkpoint!

		final CheckpointStorageLocation checkpointStorageLocation;
		final long checkpointID;

		try {
			// this must happen outside the coordinator-wide lock, because it communicates
			// with external services (in HA mode) and may block for a while.
			//checkpointID用CAS递增
			checkpointID = checkpointIdCounter.getAndIncrement();

          //初始化checkpoint存储位置
			checkpointStorageLocation = props.isSavepoint() ?
					checkpointStorage.initializeLocationForSavepoint(checkpointID, externalSavepointLocation) :
					checkpointStorage.initializeLocationForCheckpoint(checkpointID);
		}
		catch (Throwable t) {
			int numUnsuccessful = numUnsuccessfulCheckpointsTriggers.incrementAndGet();
			LOG.warn("Failed to trigger checkpoint for job {} ({} consecutive failed attempts so far).",
					job,
					numUnsuccessful,
					t);
			throw new CheckpointException(CheckpointFailureReason.EXCEPTION, t);
		}

//初始化PendingCheckpoint，中间状态的checkpoint
		final PendingCheckpoint checkpoint = new PendingCheckpoint(
			job,
			checkpointID,
			timestamp,
			ackTasks,
			masterHooks.keySet(),
			props,
			checkpointStorageLocation,
			executor);

		if (statsTracker != null) {
			PendingCheckpointStats callback = statsTracker.reportPendingCheckpoint(
				checkpointID,
				timestamp,
				props);

			checkpoint.setStatsCallback(callback);
		}

   //定时清除已过期的checkpoints
		// schedule the timer that will clean up the expired checkpoints
		final Runnable canceller = () -> {
			synchronized (lock) {
				// only do the work if the checkpoint is not discarded anyways
				// note that checkpoint completion discards the pending checkpoint object
				if (!checkpoint.isDiscarded()) {
					LOG.info("Checkpoint {} of job {} expired before completing.", checkpointID, job);

					failPendingCheckpoint(checkpoint, CheckpointFailureReason.CHECKPOINT_EXPIRED);
					pendingCheckpoints.remove(checkpointID);
					rememberRecentCheckpointId(checkpointID);

					triggerQueuedRequests();
				}
			}
		};

		try {
			// re-acquire the coordinator-wide lock
			synchronized (lock) {
			//二次检查
				preCheckBeforeTriggeringCheckpoint(isPeriodic, props.forceCheckpoint());

				LOG.info("Triggering checkpoint {} @ {} for job {}.", checkpointID, timestamp, job);

				pendingCheckpoints.put(checkpointID, checkpoint);

				ScheduledFuture<?> cancellerHandle = timer.schedule(
						canceller,
						checkpointTimeout, TimeUnit.MILLISECONDS);

				if (!checkpoint.setCancellerHandle(cancellerHandle)) {
					// checkpoint is already disposed!
					cancellerHandle.cancel(false);
				}

				// TODO, asynchronously snapshots master hook without waiting here
				//回调用户函数
				for (MasterTriggerRestoreHook<?> masterHook : masterHooks.values()) {
					final MasterState masterState =
						MasterHooks.triggerHook(masterHook, checkpointID, timestamp, executor)
							.get(checkpointTimeout, TimeUnit.MILLISECONDS);
					checkpoint.acknowledgeMasterState(masterHook.getIdentifier(), masterState);
				}
				Preconditions.checkState(checkpoint.areMasterStatesFullyAcknowledged());
			}
			// end of lock scope

			final CheckpointOptions checkpointOptions = new CheckpointOptions(
					props.getCheckpointType(),
					checkpointStorageLocation.getLocationReference());

			// send the messages to the tasks that trigger their checkpoint
			//触发tasksToTrigger的task发出checkpoint
			for (Execution execution: executions) {
				if (props.isSynchronous()) {
					execution.triggerSynchronousSavepoint(checkpointID, timestamp, checkpointOptions, advanceToEndOfTime);
				} else {
					execution.triggerCheckpoint(checkpointID, timestamp, checkpointOptions);
				}
			}

			numUnsuccessfulCheckpointsTriggers.set(0);
			return checkpoint.getCompletionFuture();
		}
		catch (Throwable t) {
			// guard the map against concurrent modifications
			synchronized (lock) {
				pendingCheckpoints.remove(checkpointID);
			}

			int numUnsuccessful = numUnsuccessfulCheckpointsTriggers.incrementAndGet();
			LOG.warn("Failed to trigger checkpoint {} for job {}. ({} consecutive failed attempts so far)",
					checkpointID, job, numUnsuccessful, t);

			if (!checkpoint.isDiscarded()) {
				failPendingCheckpoint(checkpoint, CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE, t);
			}

			try {
				checkpointStorageLocation.disposeOnFailure();
			}
			catch (Throwable t2) {
				LOG.warn("Cannot dispose failed checkpoint storage location {}", checkpointStorageLocation, t2);
			}

			// rethrow the CheckpointException directly.
			if (t instanceof CheckpointException) {
				throw (CheckpointException) t;
			}
			throw new CheckpointException(CheckpointFailureReason.EXCEPTION, t);
		}
	}
```

tasksToTrigger的task的触发Checkpoint，调用Execution的triggerCheckpoint

```
public void triggerCheckpoint(long checkpointId, long timestamp, CheckpointOptions checkpointOptions) {
   triggerCheckpointHelper(checkpointId, timestamp, checkpointOptions, false);
}

private void triggerCheckpointHelper(long checkpointId, long timestamp, CheckpointOptions checkpointOptions, boolean advanceToEndOfEventTime) {

		final CheckpointType checkpointType = checkpointOptions.getCheckpointType();
		if (advanceToEndOfEventTime && !(checkpointType.isSynchronous() && checkpointType.isSavepoint())) {
			throw new IllegalArgumentException("Only synchronous savepoints are allowed to advance the watermark to MAX.");
		}

		final LogicalSlot slot = assignedResource;

		if (slot != null) {
		//通过slot资源信息获取TaskManagerGateway并发送triggerCheckpoint请求
			final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

			taskManagerGateway.triggerCheckpoint(attemptId, getVertex().getJobId(), checkpointId, timestamp, checkpointOptions, advanceToEndOfEventTime);
		} else {
			LOG.debug("The execution has no slot assigned. This indicates that the execution is no longer running.");
		}
	}
```

TaskManagerGateway转发TaskExecutorGateway，调用Gateway的triggerCheckpoint

```
public CompletableFuture<Acknowledge> triggerCheckpoint(
      ExecutionAttemptID executionAttemptID,
      long checkpointId,
      long checkpointTimestamp,
      CheckpointOptions checkpointOptions,
      boolean advanceToEndOfEventTime) {
   log.debug("Trigger checkpoint {}@{} for {}.", checkpointId, checkpointTimestamp, executionAttemptID);

   final CheckpointType checkpointType = checkpointOptions.getCheckpointType();
   if (advanceToEndOfEventTime && !(checkpointType.isSynchronous() && checkpointType.isSavepoint())) {
      throw new IllegalArgumentException("Only synchronous savepoints are allowed to advance the watermark to MAX.");
   }

//因为一个 TaskExecutor 中可能有多个 Task 正在运行，所以获取executionAttemptID对应的task
   final Task task = taskSlotTable.getTask(executionAttemptID);

   if (task != null) {
   //task发出CheckpointBarrier
      task.triggerCheckpointBarrier(checkpointId, checkpointTimestamp, checkpointOptions, advanceToEndOfEventTime);

      return CompletableFuture.completedFuture(Acknowledge.get());
   } else {
      final String message = "TaskManager received a checkpoint request for unknown task " + executionAttemptID + '.';

      log.debug(message);
      return FutureUtils.completedExceptionally(new CheckpointException(message, CheckpointFailureReason.TASK_CHECKPOINT_FAILURE));
   }
}
```

Task发出CheckpointBarrier，只有作为 source 的 Task 才会触发 `triggerCheckpointBarrier()` 方法的调用。

```
public void triggerCheckpointBarrier(
			final long checkpointID,
			final long checkpointTimestamp,
			final CheckpointOptions checkpointOptions,
			final boolean advanceToEndOfEventTime) {

		final AbstractInvokable invokable = this.invokable;
		final CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointID, checkpointTimestamp);

		if (executionState == ExecutionState.RUNNING && invokable != null) {
			try {
			//异步执行triggerCheckpoint
				invokable.triggerCheckpointAsync(checkpointMetaData, checkpointOptions, advanceToEndOfEventTime);
			}
			catch (RejectedExecutionException ex) {
				// This may happen if the mailbox is closed. It means that the task is shutting down, so we just ignore it.
				LOG.debug(
					"Triggering checkpoint {} for {} ({}) was rejected by the mailbox",
					checkpointID, taskNameWithSubtask, executionId);
			}
			catch (Throwable t) {
				if (getExecutionState() == ExecutionState.RUNNING) {
					failExternally(new Exception(
						"Error while triggering checkpoint " + checkpointID + " for " +
							taskNameWithSubtask, t));
				} else {
					LOG.debug("Encountered error while triggering checkpoint {} for " +
						"{} ({}) while being not in state running.", checkpointID,
						taskNameWithSubtask, executionId, t);
				}
			}
		}
		else {
			LOG.debug("Declining checkpoint request for non-running task {} ({}).", taskNameWithSubtask, executionId);

			// send back a message that we did not do the checkpoint
			checkpointResponder.declineCheckpoint(jobId, executionId, checkpointID,
					new CheckpointException("Task name with subtask : " + taskNameWithSubtask, CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_NOT_READY));
		}
	}
```

执行AbstractInvokable的triggerCheckpointAsync，最终调用StreamTask的triggerCheckpointAsync，triggerCheckpointAsync向mailboxProcessor发送triggerCheckpoint的事件，从而实现异步处理

```
public Future<Boolean> triggerCheckpointAsync(
      CheckpointMetaData checkpointMetaData,
      CheckpointOptions checkpointOptions,
      boolean advanceToEndOfEventTime) {

   return mailboxProcessor.getMainMailboxExecutor().submit(
         () -> triggerCheckpoint(checkpointMetaData, checkpointOptions, advanceToEndOfEventTime),
         "checkpoint %s with %s",
      checkpointMetaData,
      checkpointOptions);
}


private boolean triggerCheckpoint(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			boolean advanceToEndOfEventTime) throws Exception {
		try {
			// No alignment if we inject a checkpoint
			CheckpointMetrics checkpointMetrics = new CheckpointMetrics()
				.setBytesBufferedInAlignment(0L)
				.setAlignmentDurationNanos(0L);

			boolean success = performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics, advanceToEndOfEventTime);
			if (!success) {
				declineCheckpoint(checkpointMetaData.getCheckpointId());
			}
			return success;
		} 
	}
	
	
private boolean performCheckpoint(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics,
			boolean advanceToEndOfTime) throws Exception {

		LOG.debug("Starting checkpoint ({}) {} on task {}",
			checkpointMetaData.getCheckpointId(), checkpointOptions.getCheckpointType(), getName());

		final long checkpointId = checkpointMetaData.getCheckpointId();

		if (isRunning) {
			actionExecutor.runThrowing(() -> {

				if (checkpointOptions.getCheckpointType().isSynchronous()) {
					setSynchronousSavepointId(checkpointId);

					if (advanceToEndOfTime) {
						advanceToEndOfEventTime();
					}
				}

				// All of the following steps happen as an atomic step from the perspective of barriers and
				// records/watermarks/timers/callbacks.
				// We generally try to emit the checkpoint barrier as soon as possible to not affect downstream
				// checkpoint alignments

				// Step (1): Prepare the checkpoint, allow operators to do some pre-barrier work.
				//           The pre-barrier work should be nothing or minimal in the common case.
				//准备发送Barrier
				operatorChain.prepareSnapshotPreBarrier(checkpointId);

				// Step (2): Send the checkpoint barrier downstream
				//向下游发送 barrier
				operatorChain.broadcastCheckpointBarrier(
						checkpointId,
						checkpointMetaData.getTimestamp(),
						checkpointOptions);

				// Step (3): Take the state snapshot. This should be largely asynchronous, to not
				//           impact progress of the streaming topology
				//存储检查点快照
				checkpointState(checkpointMetaData, checkpointOptions, checkpointMetrics);

			});

			return true;
		} else {
			actionExecutor.runThrowing(() -> {
				// we cannot perform our checkpoint - let the downstream operators know that they
				// should not wait for any input from this operator

				// we cannot broadcast the cancellation markers on the 'operator chain', because it may not
				// yet be created
				final CancelCheckpointMarker message = new CancelCheckpointMarker(checkpointMetaData.getCheckpointId());
				recordWriter.broadcastEvent(message);
			});

			return false;
		}
	}
```

OperatorChain类

```
public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
   // go forward through the operator chain and tell each operator
   // to prepare the checkpoint
   final StreamOperator<?>[] operators = this.allOperators;
   for (int i = operators.length - 1; i >= 0; --i) {
      final StreamOperator<?> op = operators[i];
      if (op != null) {
      //用户函数实现
         op.prepareSnapshotPreBarrier(checkpointId);
      }
   }
}

public void broadcastCheckpointBarrier(long id, long timestamp, CheckpointOptions checkpointOptions) throws IOException {
        //创建一个 CheckpointBarrier
		CheckpointBarrier barrier = new CheckpointBarrier(id, timestamp, checkpointOptions);
		for (RecordWriterOutput<?> streamOutput : streamOutputs) {
		//向所有的下游发送
			streamOutput.broadcastEvent(barrier);
		}
	}
	
RecordWriterOutput:
public void broadcastEvent(AbstractEvent event) throws IOException {
		recordWriter.broadcastEvent(event);
	}
	
RecordWriter:
//barrier被发送至ResultPartition
public void broadcastEvent(AbstractEvent event) throws IOException {
		try (BufferConsumer eventBufferConsumer = EventSerializer.toBufferConsumer(event)) {
			for (int targetChannel = 0; targetChannel < numberOfChannels; targetChannel++) {
				tryFinishCurrentBufferBuilder(targetChannel);

				// Retain the buffer so that it can be recycled by each channel of targetPartition
				targetPartition.addBufferConsumer(eventBufferConsumer.copy(), targetChannel);
			}

			if (flushAlways) {
				flushAll();
			}
		}
	}
```

我们已经知道，每一个 Task 的通过 `InputGate` 消费上游 Task 产生的数据，而实际上在 `StreamInputProcessor` 和 `StreamTwoInputProcessor` 中会创建 `CheckpointBarrierHandler`, `CheckpointBarrierHandler` 有两个具体的实现，即 CheckpointBarrierTracker和 CheckpointBarrierAligner，分别对应 AT_LEAST_ONCE 和 EXACTLY_ONCE 这两种模式。

`StreamInputProcessor` 和 `StreamTwoInputProcessor` 循环调用 `CheckpointBarrierHandler.getNextNonBlocked()` 获取新数据，因而在 `CheckpointBarrierHandler` 获得 `CheckpointBarrier` 后可以及时地进行 checkpoint 相关的操作。

回顾OneInputStreamTask的初始化过程，会创建CheckpointedInputGate用来接收Checkpoint

```
public void init() throws Exception {
   StreamConfig configuration = getConfiguration();
   int numberOfInputs = configuration.getNumberOfInputs();

   if (numberOfInputs > 0) {
      CheckpointedInputGate inputGate = createCheckpointedInputGate();
      TaskIOMetricGroup taskIOMetricGroup = getEnvironment().getMetricGroup().getIOMetricGroup();
      taskIOMetricGroup.gauge("checkpointAlignmentTime", inputGate::getAlignmentDurationNanos);

      DataOutput<IN> output = createDataOutput();
      StreamTaskInput<IN> input = createTaskInput(inputGate, output);
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
```

创建CheckpointedInputGate，

```
private CheckpointedInputGate createCheckpointedInputGate() throws IOException {
   InputGate[] inputGates = getEnvironment().getAllInputGates();
   InputGate inputGate = InputGateUtil.createInputGate(inputGates);

   return InputProcessorUtil.createCheckpointedInputGate(
      this,
      configuration.getCheckpointMode(),
      getEnvironment().getIOManager(),
      inputGate,
      getEnvironment().getTaskManagerInfo().getConfiguration(),
      getTaskNameWithSubtaskAndId());
}
```

InputProcessorUtil.createCheckpointedInputGate

```
public class InputProcessorUtil {

   public static CheckpointedInputGate createCheckpointedInputGate(
         AbstractInvokable toNotifyOnCheckpoint,
         CheckpointingMode checkpointMode,
         IOManager ioManager,
         InputGate inputGate,
         Configuration taskManagerConfig,
         String taskName) throws IOException {

      int pageSize = ConfigurationParserUtils.getPageSize(taskManagerConfig);

//创建用于barrier对齐的数据缓存
      BufferStorage bufferStorage = createBufferStorage(
         checkpointMode, ioManager, pageSize, taskManagerConfig, taskName);
         //创建CheckpointBarrierHandler
      CheckpointBarrierHandler barrierHandler = createCheckpointBarrierHandler(
         checkpointMode, inputGate.getNumberOfInputChannels(), taskName, toNotifyOnCheckpoint);
      return new CheckpointedInputGate(inputGate, bufferStorage, barrierHandler);
   }
```

EXACTLY_ONCE才需要BufferStorage，AT_LEAST_ONCE不需要

```
private static BufferStorage createBufferStorage(
      CheckpointingMode checkpointMode,
      IOManager ioManager,
      int pageSize,
      Configuration taskManagerConfig,
      String taskName) {
   switch (checkpointMode) {
      case EXACTLY_ONCE: {
         long maxAlign = taskManagerConfig.getLong(TaskManagerOptions.TASK_CHECKPOINT_ALIGNMENT_BYTES_LIMIT);
         if (!(maxAlign == -1 || maxAlign > 0)) {
            throw new IllegalConfigurationException(
               TaskManagerOptions.TASK_CHECKPOINT_ALIGNMENT_BYTES_LIMIT.key()
                  + " must be positive or -1 (infinite)");
         }
         return new CachedBufferStorage(pageSize, maxAlign, taskName);
      }
      case AT_LEAST_ONCE:
         return new EmptyBufferStorage();
      default:
         throw new UnsupportedOperationException("Unrecognized Checkpointing Mode: " + checkpointMode);
   }
}
```

EXACTLY_ONCE创建CheckpointBarrierAligner，AT_LEAST_ONCE创建CheckpointBarrierTracker

```
private static CheckpointBarrierHandler createCheckpointBarrierHandler(
      CheckpointingMode checkpointMode,
      int numberOfInputChannels,
      String taskName,
      AbstractInvokable toNotifyOnCheckpoint) {
   switch (checkpointMode) {
      case EXACTLY_ONCE:
         return new CheckpointBarrierAligner(
            numberOfInputChannels,
            taskName,
            toNotifyOnCheckpoint);
      case AT_LEAST_ONCE:
         return new CheckpointBarrierTracker(numberOfInputChannels, toNotifyOnCheckpoint);
      default:
         throw new UnsupportedOperationException("Unrecognized Checkpointing Mode: " + checkpointMode);
   }
}
```

而在`StreamTwoInputProcessor` 的执行阶段会调用checkpointedInputGate.pollNext()循环获取事件

```
public InputStatus processInput() throws Exception {
   InputStatus status = input.emitNext(output);

  ......
   return status;
}

public InputStatus emitNext(DataOutput<T> output) throws Exception {

		......

			Optional<BufferOrEvent> bufferOrEvent = checkpointedInputGate.pollNext();
	......
	}
```

pollNext

```
public Optional<BufferOrEvent> pollNext() throws Exception {
   while (true) {
      // process buffered BufferOrEvents before grabbing new ones
      //如果bufferStorage不为空，则优先处理bufferStorage的数据
      Optional<BufferOrEvent> next;
      if (bufferStorage.isEmpty()) {
         next = inputGate.pollNext();
      }
      else {
         // TODO: FLINK-12536 for non credit-based flow control, getNext method is blocking
         next = bufferStorage.pollNext();
         if (!next.isPresent()) {
            return pollNext();
         }
      }

      if (!next.isPresent()) {
         return handleEmptyBuffer();
      }

      BufferOrEvent bufferOrEvent = next.get();
      
      if (barrierHandler.isBlocked(offsetChannelIndex(bufferOrEvent.getChannelIndex()))) {
      //如果是EXACTLY_ONCE的CheckpointBarrierAligner，则阻塞，将消息刚入bufferStorage
         // if the channel is blocked, we just store the BufferOrEvent
         bufferStorage.add(bufferOrEvent);
         if (bufferStorage.isFull()) {
            barrierHandler.checkpointSizeLimitExceeded(bufferStorage.getMaxBufferedBytes());
            //如果bufferStorage满了，则开启一个新的存储队列
            bufferStorage.rollOver();
         }
      }
      else if (bufferOrEvent.isBuffer()) {
         return next;
      }
      //如果是CheckpointBarrier数据，则调用barrierHandler.processBarrier
      else if (bufferOrEvent.getEvent().getClass() == CheckpointBarrier.class) {
         CheckpointBarrier checkpointBarrier = (CheckpointBarrier) bufferOrEvent.getEvent();
         if (!endOfInputGate) {
            // process barriers only if there is a chance of the checkpoint completing
            if (barrierHandler.processBarrier(checkpointBarrier, offsetChannelIndex(bufferOrEvent.getChannelIndex()), bufferStorage.getPendingBytes())) {
               bufferStorage.rollOver();
            }
         }
      }
      else if (bufferOrEvent.getEvent().getClass() == CancelCheckpointMarker.class) {
         if (barrierHandler.processCancellationBarrier((CancelCheckpointMarker) bufferOrEvent.getEvent())) {
            bufferStorage.rollOver();
         }
      }
      else {
         if (bufferOrEvent.getEvent().getClass() == EndOfPartitionEvent.class) {
            if (barrierHandler.processEndOfPartition()) {
               bufferStorage.rollOver();
            }
         }
         return next;
      }
   }
}
```

AT_LEAST_ONCE的CheckpointBarrierTracker的processBarrier过程，它仅仅追踪从每一个 input channel 接收到的 barrier，当所有 input channel 的 barrier 都被接收时，就可以触发 checkpoint 了

```
public boolean processBarrier(CheckpointBarrier receivedBarrier, int channelIndex, long bufferedBytes) throws Exception {
   final long barrierId = receivedBarrier.getId();

   // fast path for single channel trackers
   if (totalNumberOfInputChannels == 1) {
      notifyCheckpoint(receivedBarrier, 0, 0);
      return false;
   }

   // general path for multiple input channels
   if (LOG.isDebugEnabled()) {
      LOG.debug("Received barrier for checkpoint {} from channel {}", barrierId, channelIndex);
   }

   // find the checkpoint barrier in the queue of pending barriers
   CheckpointBarrierCount barrierCount = null;
   int pos = 0;

   for (CheckpointBarrierCount next : pendingCheckpoints) {
      if (next.checkpointId == barrierId) {
         barrierCount = next;
         break;
      }
      pos++;
   }

   if (barrierCount != null) {
      // add one to the count to that barrier and check for completion
      int numBarriersNew = barrierCount.incrementBarrierCount();
      if (numBarriersNew == totalNumberOfInputChannels) {
      // 在当前 barrierId 前面的所有未完成的 checkpoint 都可以丢弃了
         // checkpoint can be triggered (or is aborted and all barriers have been seen)
         // first, remove this checkpoint and all all prior pending
         // checkpoints (which are now subsumed)
         for (int i = 0; i <= pos; i++) {
            pendingCheckpoints.pollFirst();
         }

         // notify the listener
         if (!barrierCount.isAborted()) {
            if (LOG.isDebugEnabled()) {
               LOG.debug("Received all barriers for checkpoint {}", barrierId);
            }

//通知进行 checkpoint
            notifyCheckpoint(receivedBarrier, 0, 0);
         }
      }
   }
   else {
      // first barrier for that checkpoint ID
      // add it only if it is newer than the latest checkpoint.
      // if it is not newer than the latest checkpoint ID, then there cannot be a
      // successful checkpoint for that ID anyways
      if (barrierId > latestPendingCheckpointID) {
         latestPendingCheckpointID = barrierId;
         pendingCheckpoints.addLast(new CheckpointBarrierCount(barrierId));

         // make sure we do not track too many checkpoints
         if (pendingCheckpoints.size() > MAX_CHECKPOINTS_TO_TRACK) {
            pendingCheckpoints.pollFirst();
         }
      }
   }
   return false;
}
```

而对于 EXACTLY_ONCE 模式下的 `BarrierBuffer`，它除了要追踪每一个 input channel 接收到的 barrier 之外，在接收到所有的 barrier 之前，先收到 barrier 的 channel 要进入阻塞状态。当然为了避免进入“反压”状态，`BarrierBuffer` 会继续接收数据，但会对接收到的数据进行缓存，直到所有的 barrier 都到达。

```
public boolean processBarrier(CheckpointBarrier receivedBarrier, int channelIndex, long bufferedBytes) throws Exception {
   final long barrierId = receivedBarrier.getId();

   // fast path for single channel cases
   //只有一个InputChannel，即只有1个barrier
   if (totalNumberOfInputChannels == 1) {
      if (barrierId > currentCheckpointId) {
         // new checkpoint
         currentCheckpointId = barrierId;
         notifyCheckpoint(receivedBarrier, bufferedBytes, latestAlignmentDurationNanos);
      }
      return false;
   }

   boolean checkpointAborted = false;

   // -- general code path for multiple input channels --

   if (numBarriersReceived > 0) {
   //已经收到了多个Barriers
      // this is only true if some alignment is already progress and was not canceled

      if (barrierId == currentCheckpointId) {
         // regular case
         //barrierId是当前正在对齐的barrierId
         onBarrier(channelIndex);
      }
      else if (barrierId > currentCheckpointId) {
      //barrierId比正在对齐的barrierId大，则废除当前正在做的Checkpoint
         // we did not complete the current checkpoint, another started before
         LOG.warn("{}: Received checkpoint barrier for checkpoint {} before completing current checkpoint {}. " +
               "Skipping current checkpoint.",
            taskName,
            barrierId,
            currentCheckpointId);

         // let the task know we are not completing this
         notifyAbort(currentCheckpointId,
            new CheckpointException(
               "Barrier id: " + barrierId,
               CheckpointFailureReason.CHECKPOINT_DECLINED_SUBSUMED));

         // abort the current checkpoint
         releaseBlocksAndResetBarriers();
         checkpointAborted = true;

         // begin a the new checkpoint
         //重新开始对齐
         beginNewAlignment(barrierId, channelIndex);
      }
      else {
         // ignore trailing barrier from an earlier checkpoint (obsolete now)
         return false;
      }
   }
   else if (barrierId > currentCheckpointId) {
      // first barrier of a new checkpoint
      //收到了第一个barrier并且之前对齐的barrierId大
      beginNewAlignment(barrierId, channelIndex);
   }
   else {
      // either the current checkpoint was canceled (numBarriers == 0) or
      // this barrier is from an old subsumed checkpoint
      return false;
   }

   // check if we have all barriers - since canceled checkpoints always have zero barriers
   // this can only happen on a non canceled checkpoint
   //是否收到了所有InputChannel的barrier
   if (numBarriersReceived + numClosedChannels == totalNumberOfInputChannels) {
      // actually trigger checkpoint
      if (LOG.isDebugEnabled()) {
         LOG.debug("{}: Received all barriers, triggering checkpoint {} at {}.",
            taskName,
            receivedBarrier.getId(),
            receivedBarrier.getTimestamp());
      }
  //释放缓存，并通知进行 checkpoint
      releaseBlocksAndResetBarriers();
      notifyCheckpoint(receivedBarrier, bufferedBytes, latestAlignmentDurationNanos);
      return true;
   }
   return checkpointAborted;
}

//阻塞已收到barrier的InputChannel
protected void onBarrier(int channelIndex) throws IOException {
		if (!blockedChannels[channelIndex]) {
			blockedChannels[channelIndex] = true;

			numBarriersReceived++;

			if (LOG.isDebugEnabled()) {
				LOG.debug("{}: Received barrier from channel {}.", taskName, channelIndex);
			}
		}
		else {
			throw new IOException("Stream corrupt: Repeated barrier for same checkpoint on input " + channelIndex);
		}
	}
	
//开启新的对齐，更新currentCheckpointId，并阻塞InputChannel
protected void beginNewAlignment(long checkpointId, int channelIndex) throws IOException {
		currentCheckpointId = checkpointId;
		onBarrier(channelIndex);

		startOfAlignmentTimestamp = System.nanoTime();

		if (LOG.isDebugEnabled()) {
			LOG.debug("{}: Starting stream alignment for checkpoint {}.", taskName, checkpointId);
		}
	}
	
//将InputChannel变为非阻塞，并计算对齐耗时
public void releaseBlocksAndResetBarriers() {
		LOG.debug("{}: End of stream alignment, feeding buffered data back.", taskName);

		for (int i = 0; i < blockedChannels.length; i++) {
			blockedChannels[i] = false;
		}

		// the next barrier that comes must assume it is the first
		numBarriersReceived = 0;

		if (startOfAlignmentTimestamp > 0) {
			latestAlignmentDurationNanos = System.nanoTime() - startOfAlignmentTimestamp;
			startOfAlignmentTimestamp = 0;
		}
	}
```

调用CheckpointBarrierHandler的notifyCheckpoint

```
protected void notifyCheckpoint(CheckpointBarrier checkpointBarrier, long bufferedBytes, long alignmentDurationNanos) throws Exception {
   if (toNotifyOnCheckpoint != null) {
      CheckpointMetaData checkpointMetaData =
         new CheckpointMetaData(checkpointBarrier.getId(), checkpointBarrier.getTimestamp());

      CheckpointMetrics checkpointMetrics = new CheckpointMetrics()
         .setBytesBufferedInAlignment(bufferedBytes)
         .setAlignmentDurationNanos(alignmentDurationNanos);

      toNotifyOnCheckpoint.triggerCheckpointOnBarrier(
         checkpointMetaData,
         checkpointBarrier.getCheckpointOptions(),
         checkpointMetrics);
   }
}
```

调用AbstractInvokable的triggerCheckpointOnBarrier，即StreamTask的triggerCheckpointOnBarrier，triggerCheckpointOnBarrier与triggerCheckpointAsync基本一样，其中 triggerCheckpointAsync是触发 checkpoint 的源头，会向下游注入 `CheckpointBarrier`；而下游的其他任务在收到 `CheckpointBarrier` 后调用 `triggerCheckpointOnBarrier` 方法。这两个方法的具体实现有一些细微的差异，但主要的逻辑是一致的，在 `StreamTask.performCheckpoint()` 方法中

```
public void triggerCheckpointOnBarrier(
      CheckpointMetaData checkpointMetaData,
      CheckpointOptions checkpointOptions,
      CheckpointMetrics checkpointMetrics) throws Exception {

   try {
      if (performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics, false)) {
         if (isSynchronousSavepointId(checkpointMetaData.getCheckpointId())) {
            runSynchronousSavepointMailboxLoop();
         }
      }
   }
   catch (CancelTaskException e) {
      LOG.info("Operator {} was cancelled while performing checkpoint {}.",
            getName(), checkpointMetaData.getCheckpointId());
      throw e;
   }
   catch (Exception e) {
      throw new Exception("Could not perform checkpoint " + checkpointMetaData.getCheckpointId() + " for operator " +
         getName() + '.', e);
   }
}
```

回顾performCheckpoint方法，发送Checkpoint经历三个步骤，准备发送，向下游发送Checkpoint，存储检查点快照状态。刚才的步骤是向下游发送Checkpoint，接下来看下怎么存储快照，就是StreamTask的checkpointState

```
private void checkpointState(
      CheckpointMetaData checkpointMetaData,
      CheckpointOptions checkpointOptions,
      CheckpointMetrics checkpointMetrics) throws Exception {

//1. 解析得到 CheckpointStorageLocation
   CheckpointStreamFactory storage = checkpointStorage.resolveCheckpointStorageLocation(
         checkpointMetaData.getCheckpointId(),
         checkpointOptions.getTargetLocation());

	//2. 将存储过程封装为 CheckpointingOperation，开始进行检查点存储操作
   CheckpointingOperation checkpointingOperation = new CheckpointingOperation(
      this,
      checkpointMetaData,
      checkpointOptions,
      storage,
      checkpointMetrics);

   checkpointingOperation.executeCheckpointing();
}
```

检查点快照的过程被封装为 `CheckpointingOperation`，由于每一个 `StreamTask` 可能包含多个算子，因而内部使用一个 Map 维护 `OperatorID -> OperatorSnapshotFutures` 的关系。`CheckpointingOperation`中，快照操作分为两个阶段，第一阶段是同步执行的，第二阶段是异步执行的：

```
private static final class CheckpointingOperation {

   private final StreamTask<?, ?> owner;

   private final CheckpointMetaData checkpointMetaData;
   private final CheckpointOptions checkpointOptions;
   private final CheckpointMetrics checkpointMetrics;
   private final CheckpointStreamFactory storageLocation;

   private final StreamOperator<?>[] allOperators;

   private long startSyncPartNano;
   private long startAsyncPartNano;

   // ------------------------
//每一个算子的快照被抽象为 OperatorSnapshotFutures，包含了 operator state 和 keyed state 的快照结果：
   private final Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress;

   public void executeCheckpointing() throws Exception {
      startSyncPartNano = System.nanoTime();

      try {
      //1. 同步执行的部分
         for (StreamOperator<?> op : allOperators) {
            checkpointStreamOperator(op);
         }

         if (LOG.isDebugEnabled()) {
            LOG.debug("Finished synchronous checkpoints for checkpoint {} on task {}",
               checkpointMetaData.getCheckpointId(), owner.getName());
         }

         startAsyncPartNano = System.nanoTime();

         checkpointMetrics.setSyncDurationMillis((startAsyncPartNano - startSyncPartNano) / 1_000_000);

         // we are transferring ownership over snapshotInProgressList for cleanup to the thread, active on submit
         //2. 异步执行的部分
				// checkpoint 可以配置成同步执行，也可以配置成异步执行的
				// 如果是同步执行的，在这里实际上所有的 runnable future 都是已经完成的状态
         AsyncCheckpointRunnable asyncCheckpointRunnable = new AsyncCheckpointRunnable(
            owner,
            operatorSnapshotsInProgress,
            checkpointMetaData,
            checkpointMetrics,
            startAsyncPartNano);

         owner.cancelables.registerCloseable(asyncCheckpointRunnable);
         owner.asyncOperationsThreadPool.execute(asyncCheckpointRunnable);

         if (LOG.isDebugEnabled()) {
            LOG.debug("{} - finished synchronous part of checkpoint {}. " +
                  "Alignment duration: {} ms, snapshot duration {} ms",
               owner.getName(), checkpointMetaData.getCheckpointId(),
               checkpointMetrics.getAlignmentDurationNanos() / 1_000_000,
               checkpointMetrics.getSyncDurationMillis());
         }
      } 
   }
```

每一个算子的快照被抽象为 `OperatorSnapshotFutures`，包含了 operator state 和 keyed state 的快照结果

```
public class OperatorSnapshotFutures {

   @Nonnull
   private RunnableFuture<SnapshotResult<KeyedStateHandle>> keyedStateManagedFuture;

   @Nonnull
   private RunnableFuture<SnapshotResult<KeyedStateHandle>> keyedStateRawFuture;

   @Nonnull
   private RunnableFuture<SnapshotResult<OperatorStateHandle>> operatorStateManagedFuture;

   @Nonnull
   private RunnableFuture<SnapshotResult<OperatorStateHandle>> operatorStateRawFuture;
```

checkpointStreamOperator

```
private void checkpointStreamOperator(StreamOperator<?> op) throws Exception {
   if (null != op) {
// 调用 StreamOperator.snapshotState 方法进行快照
				// 返回的结果是 runnable future，可能是已经执行完了，也可能没有执行完
      OperatorSnapshotFutures snapshotInProgress = op.snapshotState(
            checkpointMetaData.getCheckpointId(),
            checkpointMetaData.getTimestamp(),
            checkpointOptions,
            storageLocation);
      operatorSnapshotsInProgress.put(op.getOperatorID(), snapshotInProgress);
   }
}
```

在同步执行阶段，会依次调用每一个算子的 `StreamOperator.snapshotState`，返回结果是一个 runnable future。根据 checkpoint 配置成同步模式和异步模式的区别，这个 future 可能处于完成状态，也可能处于未完成状态：

```
StreamOperator：
OperatorSnapshotFutures snapshotState(
   long checkpointId,
   long timestamp,
   CheckpointOptions checkpointOptions,
   CheckpointStreamFactory storageLocation) throws Exception;
   

AbstractStreamOperator：
@Override
	public final OperatorSnapshotFutures snapshotState(long checkpointId, long timestamp, CheckpointOptions checkpointOptions,
			CheckpointStreamFactory factory) throws Exception {

		KeyGroupRange keyGroupRange = null != keyedStateBackend ?
				keyedStateBackend.getKeyGroupRange() : KeyGroupRange.EMPTY_KEY_GROUP_RANGE;

		OperatorSnapshotFutures snapshotInProgress = new OperatorSnapshotFutures();

		StateSnapshotContextSynchronousImpl snapshotContext = new StateSnapshotContextSynchronousImpl(
			checkpointId,
			timestamp,
			factory,
			keyGroupRange,
			getContainingTask().getCancelables());

		try {
		//对状态进行快照
			snapshotState(snapshotContext);

		//raw state，要在子类中自己实现 raw state 的快照写入
			//timer 是作为 raw keyed state 写入的snapshotInProgress.setKeyedStateRawFuture(snapshotContext.getKeyedStateStreamFuture());
			snapshotInProgress.setOperatorStateRawFuture(snapshotContext.getOperatorStateStreamFuture());

//写入 managed state 快照
			if (null != operatorStateBackend) {
				snapshotInProgress.setOperatorStateManagedFuture(
					operatorStateBackend.snapshot(checkpointId, timestamp, factory, checkpointOptions));
			}

//写入 managed keyed state 快照
			if (null != keyedStateBackend) {
				snapshotInProgress.setKeyedStateManagedFuture(
					keyedStateBackend.snapshot(checkpointId, timestamp, factory, checkpointOptions));
			}
		} catch (Exception snapshotException) {
			try {
				snapshotInProgress.cancel();
			} catch (Exception e) {
				snapshotException.addSuppressed(e);
			}

			String snapshotFailMessage = "Could not complete snapshot " + checkpointId + " for operator " +
				getOperatorName() + ".";

			if (!getContainingTask().isCanceled()) {
				LOG.info(snapshotFailMessage, snapshotException);
			}
			try {
				snapshotContext.closeExceptionally();
			} catch (IOException e) {
				snapshotException.addSuppressed(e);
			}
			throw new CheckpointException(snapshotFailMessage, CheckpointFailureReason.CHECKPOINT_DECLINED, snapshotException);
		}

		return snapshotInProgress;
	}
	
	
	public void snapshotState(StateSnapshotContext context) throws Exception {
		final KeyedStateBackend<?> keyedStateBackend = getKeyedStateBackend();
		//TODO all of this can be removed once heap-based timers are integrated with RocksDB incremental snapshots
		// 所有的 timer 都作为 raw keyed state 写入
		if (keyedStateBackend instanceof AbstractKeyedStateBackend &&
			((AbstractKeyedStateBackend<?>) keyedStateBackend).requiresLegacySynchronousTimerSnapshots()) {

			KeyedStateCheckpointOutputStream out;

			try {
				out = context.getRawKeyedOperatorStateOutput();
			} catch (Exception exception) {
				throw new Exception("Could not open raw keyed operator state stream for " +
					getOperatorName() + '.', exception);
			}

			try {
				KeyGroupsList allKeyGroups = out.getKeyGroupList();
				for (int keyGroupIdx : allKeyGroups) {
					out.startNewKeyGroup(keyGroupIdx);

					timeServiceManager.snapshotStateForKeyGroup(
						new DataOutputViewStreamWrapper(out), keyGroupIdx);
				}
			} catch (Exception exception) {
				throw new Exception("Could not write timer service of " + getOperatorName() +
					" to checkpoint state stream.", exception);
			} finally {
				try {
					out.close();
				} catch (Exception closeException) {
					LOG.warn("Could not close raw keyed operator state stream for {}. This " +
						"might have prevented deleting some state data.", getOperatorName(), closeException);
				}
			}
		}
	}
	
	
//用户也可以自己实现snapshotState，AbstractUdfStreamOperator继承AbstractStreamOperator
public void snapshotState(StateSnapshotContext context) throws Exception {
//先调用父类AbstractStreamOperator方法，写入timer
		super.snapshotState(context);
		//通过反射调用用户函数中的快照操作
		StreamingFunctionUtils.snapshotFunctionState(context, getOperatorStateBackend(), userFunction);
	}
	
	
	StreamingFunctionUtils类：
public static void snapshotFunctionState(
			StateSnapshotContext context,
			OperatorStateBackend backend,
			Function userFunction) throws Exception {

		Preconditions.checkNotNull(context);
		Preconditions.checkNotNull(backend);

		while (true) {

			if (trySnapshotFunctionState(context, backend, userFunction)) {
				break;
			}

			// inspect if the user function is wrapped, then unwrap and try again if we can snapshot the inner function
			if (userFunction instanceof WrappingFunction) {
				userFunction = ((WrappingFunction<?>) userFunction).getWrappedFunction();
			} else {
				break;
			}
		}
	}
	
	
	
private static boolean trySnapshotFunctionState(
			StateSnapshotContext context,
			OperatorStateBackend backend,
			Function userFunction) throws Exception {

// 如果用户函数实现了 CheckpointedFunction 接口，调用 snapshotState 创建快照
		if (userFunction instanceof CheckpointedFunction) {
			((CheckpointedFunction) userFunction).snapshotState(context);

			return true;
		}

// 如果用户函数实现了 ListCheckpointed
		if (userFunction instanceof ListCheckpointed) {
		//先调用 snapshotState 方法获取当前状态
			@SuppressWarnings("unchecked")
			List<Serializable> partitionableState = ((ListCheckpointed<Serializable>) userFunction).
					snapshotState(context.getCheckpointId(), context.getCheckpointTimestamp());
//获取后端存储的状态的引用
			ListState<Serializable> listState = backend.
					getSerializableListState(DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME);
//清空当前后端存储的 ListState
			listState.clear();

//将当前状态依次加入后端存储
			if (null != partitionableState) {
				try {
					for (Serializable statePartition : partitionableState) {
						listState.add(statePartition);
					}
				} catch (Exception e) {
					listState.clear();

					throw new Exception("Could not write partitionable state to operator " +
						"state backend.", e);
				}
			}

			return true;
		}

		return false;
	}
```

现在我们已经看到 checkpoint 操作是如何同用户自定义函数建立关联的了，接下来我们来看看由 Flink 托管的状态是如何写入存储系统的。

首先来看看 operator state。`DefaultOperatorStateBackend` 将实际的工作交给 `DefaultOperatorStateBackendSnapshotStrategy` 完成。首先，会为对当前注册的所有 operator state（包含 list state 和 broadcast state）做深度拷贝，然后将实际的写入操作封装在一个异步的 FutureTask 中，这个 FutureTask 的主要任务包括： 1）打开输出流 2）写入状态元数据信息 3）写入状态 4）关闭输出流，获得状态句柄。如果不启用异步checkpoint模式，那么这个 FutureTask 在同步阶段就会立刻执行。

```
public RunnableFuture<SnapshotResult<OperatorStateHandle>> snapshot(
   final long checkpointId,
   final long timestamp,
   @Nonnull final CheckpointStreamFactory streamFactory,
   @Nonnull final CheckpointOptions checkpointOptions) throws IOException {

   if (registeredOperatorStates.isEmpty() && registeredBroadcastStates.isEmpty()) {
      return DoneFuture.of(SnapshotResult.empty());
   }

   final Map<String, PartitionableListState<?>> registeredOperatorStatesDeepCopies =
      new HashMap<>(registeredOperatorStates.size());
   final Map<String, BackendWritableBroadcastState<?, ?>> registeredBroadcastStatesDeepCopies =
      new HashMap<>(registeredBroadcastStates.size());

//获得已注册的所有 list state 和 broadcast state 的深拷贝
   ClassLoader snapshotClassLoader = Thread.currentThread().getContextClassLoader();
   Thread.currentThread().setContextClassLoader(userClassLoader);
   try {
      // eagerly create deep copies of the list and the broadcast states (if any)
      // in the synchronous phase, so that we can use them in the async writing.

      if (!registeredOperatorStates.isEmpty()) {
         for (Map.Entry<String, PartitionableListState<?>> entry : registeredOperatorStates.entrySet()) {
            PartitionableListState<?> listState = entry.getValue();
            if (null != listState) {
               listState = listState.deepCopy();
            }
            registeredOperatorStatesDeepCopies.put(entry.getKey(), listState);
         }
      }

      if (!registeredBroadcastStates.isEmpty()) {
         for (Map.Entry<String, BackendWritableBroadcastState<?, ?>> entry : registeredBroadcastStates.entrySet()) {
            BackendWritableBroadcastState<?, ?> broadcastState = entry.getValue();
            if (null != broadcastState) {
               broadcastState = broadcastState.deepCopy();
            }
            registeredBroadcastStatesDeepCopies.put(entry.getKey(), broadcastState);
         }
      }
   } finally {
      Thread.currentThread().setContextClassLoader(snapshotClassLoader);
   }

//将主要写入操作封装为一个异步的FutureTask
   AsyncSnapshotCallable<SnapshotResult<OperatorStateHandle>> snapshotCallable =
      new AsyncSnapshotCallable<SnapshotResult<OperatorStateHandle>>() {

         @Override
         protected SnapshotResult<OperatorStateHandle> callInternal() throws Exception {
// 创建状态输出流
            CheckpointStreamFactory.CheckpointStateOutputStream localOut =
               streamFactory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE);
            snapshotCloseableRegistry.registerCloseable(localOut);
// 收集元数据
            // get the registered operator state infos ...
            List<StateMetaInfoSnapshot> operatorMetaInfoSnapshots =
               new ArrayList<>(registeredOperatorStatesDeepCopies.size());

            for (Map.Entry<String, PartitionableListState<?>> entry :
               registeredOperatorStatesDeepCopies.entrySet()) {
               operatorMetaInfoSnapshots.add(entry.getValue().getStateMetaInfo().snapshot());
            }

            // ... get the registered broadcast operator state infos ...
            List<StateMetaInfoSnapshot> broadcastMetaInfoSnapshots =
               new ArrayList<>(registeredBroadcastStatesDeepCopies.size());

            for (Map.Entry<String, BackendWritableBroadcastState<?, ?>> entry :
               registeredBroadcastStatesDeepCopies.entrySet()) {
               broadcastMetaInfoSnapshots.add(entry.getValue().getStateMetaInfo().snapshot());
            }

            // ... write them all in the checkpoint stream ...
            // 写入元数据
            DataOutputView dov = new DataOutputViewStreamWrapper(localOut);

            OperatorBackendSerializationProxy backendSerializationProxy =
               new OperatorBackendSerializationProxy(operatorMetaInfoSnapshots, broadcastMetaInfoSnapshots);

            backendSerializationProxy.write(dov);

            // ... and then go for the states ...
// 写入状态
            // we put BOTH normal and broadcast state metadata here
            int initialMapCapacity =
               registeredOperatorStatesDeepCopies.size() + registeredBroadcastStatesDeepCopies.size();
            final Map<String, OperatorStateHandle.StateMetaInfo> writtenStatesMetaData =
               new HashMap<>(initialMapCapacity);

            for (Map.Entry<String, PartitionableListState<?>> entry :
               registeredOperatorStatesDeepCopies.entrySet()) {

               PartitionableListState<?> value = entry.getValue();
               long[] partitionOffsets = value.write(localOut);
               OperatorStateHandle.Mode mode = value.getStateMetaInfo().getAssignmentMode();
               writtenStatesMetaData.put(
                  entry.getKey(),
                  new OperatorStateHandle.StateMetaInfo(partitionOffsets, mode));
            }

            // ... and the broadcast states themselves ...
            for (Map.Entry<String, BackendWritableBroadcastState<?, ?>> entry :
               registeredBroadcastStatesDeepCopies.entrySet()) {

               BackendWritableBroadcastState<?, ?> value = entry.getValue();
               long[] partitionOffsets = {value.write(localOut)};
               OperatorStateHandle.Mode mode = value.getStateMetaInfo().getAssignmentMode();
               writtenStatesMetaData.put(
                  entry.getKey(),
                  new OperatorStateHandle.StateMetaInfo(partitionOffsets, mode));
            }

            // ... and, finally, create the state handle.
            OperatorStateHandle retValue = null;

            if (snapshotCloseableRegistry.unregisterCloseable(localOut)) {
//关闭输出流，获得状态句柄，后面可以用这个句柄读取状态
               StreamStateHandle stateHandle = localOut.closeAndGetHandle();

               if (stateHandle != null) {
                  retValue = new OperatorStreamStateHandle(writtenStatesMetaData, stateHandle);
               }

               return SnapshotResult.of(retValue);
            } else {
               throw new IOException("Stream was already unregistered.");
            }
         }

         @Override
         protected void cleanupProvidedResources() {
            // nothing to do
         }

         @Override
         protected void logAsyncSnapshotComplete(long startTime) {
            if (asynchronousSnapshots) {
               logAsyncCompleted(streamFactory, startTime);
            }
         }
      };

   final FutureTask<SnapshotResult<OperatorStateHandle>> task =
      snapshotCallable.toAsyncSnapshotFutureTask(closeStreamOnCancelRegistry);
//如果不是异步 checkpoint 那么在这里直接运行 FutureTask，即在同步阶段就完成了状态的写入
   if (!asynchronousSnapshots) {
      task.run();
   }

   return task;
}
```

keyed state 写入的基本流程与此相似，但由于 keyed state 在存储时有多种实现，包括基于堆内存和 RocksDB 的不同实现，此外基于 RocksDB 的实现还包括支持[增量 checkpoint](https://flink.apache.org/features/2018/01/30/incremental-checkpointing.html)，因而相比于 operator state 要更复杂一些。另外，Flink 自 1.5.0 版本还引入了一个[本地状态存储](https://issues.apache.org/jira/browse/FLINK-8360)的优化，支持在 TaskManager 的本地保存一份 keyed state，试图优化状态恢复的速度和网络开销。

具体三种 StateBackend 即MemoryStateBackend、FsStateBackend、RocksDBStateBackend对应的 OperatorStateBackend 和 KeyedStateBackend 分别为：

![](1.png)

所谓本地状态存储，即在存储检查点快照时，在 `Task` 所在的 TaskManager 本地文件系统中存储一份副本，这样在进行状态恢复时可以优先从本地状态进行恢复，从而减少网络数据传输的开销。本地状态存储仅针对 keyed state，我们以较为简单的 `HeapKeyedStateBackend` 为例，看看本地状态存储时如何实现的。

```
public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
		long checkpointId,
		long timestamp,
		@Nonnull CheckpointStreamFactory primaryStreamFactory,
		@Nonnull CheckpointOptions checkpointOptions) throws IOException {

		if (!hasRegisteredState()) {
			return DoneFuture.of(SnapshotResult.empty());
		}

		int numStates = registeredKVStates.size() + registeredPQStates.size();

		Preconditions.checkState(numStates <= Short.MAX_VALUE,
			"Too many states: " + numStates +
				". Currently at most " + Short.MAX_VALUE + " states are supported");

		final List<StateMetaInfoSnapshot> metaInfoSnapshots = new ArrayList<>(numStates);
		final Map<StateUID, Integer> stateNamesToId =
			new HashMap<>(numStates);
		final Map<StateUID, StateSnapshot> cowStateStableSnapshots =
			new HashMap<>(numStates);

		processSnapshotMetaInfoForAllStates(
			metaInfoSnapshots,
			cowStateStableSnapshots,
			stateNamesToId,
			registeredKVStates,
			StateMetaInfoSnapshot.BackendStateType.KEY_VALUE);

		processSnapshotMetaInfoForAllStates(
			metaInfoSnapshots,
			cowStateStableSnapshots,
			stateNamesToId,
			registeredPQStates,
			StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE);

		final KeyedBackendSerializationProxy<K> serializationProxy =
			new KeyedBackendSerializationProxy<>(
				// TODO: this code assumes that writing a serializer is threadsafe, we should support to
				// get a serialized form already at state registration time in the future
				getKeySerializer(),
				metaInfoSnapshots,
				!Objects.equals(UncompressedStreamCompressionDecorator.INSTANCE, keyGroupCompressionDecorator));

//创建 CheckpointStreamWithResultProvider
		final SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier =
//是否启用本地存储
			localRecoveryConfig.isLocalRecoveryEnabled() ?

				() -> CheckpointStreamWithResultProvider.createDuplicatingStream(
					checkpointId,
					CheckpointedStateScope.EXCLUSIVE,
					primaryStreamFactory,
					localRecoveryConfig.getLocalStateDirectoryProvider()) :

				() -> CheckpointStreamWithResultProvider.createSimpleStream(
					CheckpointedStateScope.EXCLUSIVE,
					primaryStreamFactory);

		//--------------------------------------------------- this becomes the end of sync part

		final AsyncSnapshotCallable<SnapshotResult<KeyedStateHandle>> asyncSnapshotCallable =
			new AsyncSnapshotCallable<SnapshotResult<KeyedStateHandle>>() {
				@Override
				protected SnapshotResult<KeyedStateHandle> callInternal() throws Exception {

					final CheckpointStreamWithResultProvider streamWithResultProvider =
						checkpointStreamSupplier.get();

					snapshotCloseableRegistry.registerCloseable(streamWithResultProvider);

					final CheckpointStreamFactory.CheckpointStateOutputStream localStream =
						streamWithResultProvider.getCheckpointOutputStream();

					final DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(localStream);
					serializationProxy.write(outView);

					final long[] keyGroupRangeOffsets = new long[keyGroupRange.getNumberOfKeyGroups()];

					for (int keyGroupPos = 0; keyGroupPos < keyGroupRange.getNumberOfKeyGroups(); ++keyGroupPos) {
						int keyGroupId = keyGroupRange.getKeyGroupId(keyGroupPos);
						keyGroupRangeOffsets[keyGroupPos] = localStream.getPos();
						outView.writeInt(keyGroupId);

						for (Map.Entry<StateUID, StateSnapshot> stateSnapshot :
							cowStateStableSnapshots.entrySet()) {
							StateSnapshot.StateKeyGroupWriter partitionedSnapshot =

								stateSnapshot.getValue().getKeyGroupWriter();
							try (
								OutputStream kgCompressionOut =
									keyGroupCompressionDecorator.decorateWithCompression(localStream)) {
								DataOutputViewStreamWrapper kgCompressionView =
									new DataOutputViewStreamWrapper(kgCompressionOut);
								kgCompressionView.writeShort(stateNamesToId.get(stateSnapshot.getKey()));
								partitionedSnapshot.writeStateInKeyGroup(kgCompressionView, keyGroupId);
							} // this will just close the outer compression stream
						}
					}

					if (snapshotCloseableRegistry.unregisterCloseable(streamWithResultProvider)) {
						KeyGroupRangeOffsets kgOffs = new KeyGroupRangeOffsets(keyGroupRange, keyGroupRangeOffsets);
						SnapshotResult<StreamStateHandle> result =
							streamWithResultProvider.closeAndFinalizeCheckpointStreamResult();
						return CheckpointStreamWithResultProvider.toKeyedStateHandleSnapshotResult(result, kgOffs);
					} else {
						throw new IOException("Stream already unregistered.");
					}
				}

				@Override
				protected void cleanupProvidedResources() {
					for (StateSnapshot tableSnapshot : cowStateStableSnapshots.values()) {
						tableSnapshot.release();
					}
				}

				@Override
				protected void logAsyncSnapshotComplete(long startTime) {
					if (snapshotStrategySynchronicityTrait.isAsynchronous()) {
						logAsyncCompleted(primaryStreamFactory, startTime);
					}
				}
			};

		final FutureTask<SnapshotResult<KeyedStateHandle>> task =
			asyncSnapshotCallable.toAsyncSnapshotFutureTask(cancelStreamRegistry);
		finalizeSnapshotBeforeReturnHook(task);

		return task;
	}
```

所以在启用本地状态存储的情况下，会创建两个输出流，其中 `primaryOut` 对应外部存储，而 `secondaryOut` 对应本地存储。状态会输出两份。本地状态句柄会存储在 `TaskLocalStateStore` 中。

CheckpointStreamWithResultProvider：

```
static CheckpointStreamWithResultProvider createDuplicatingStream(
   @Nonnegative long checkpointId,
   @Nonnull CheckpointedStateScope checkpointedStateScope,
   @Nonnull CheckpointStreamFactory primaryStreamFactory,
   @Nonnull LocalRecoveryDirectoryProvider secondaryStreamDirProvider) throws IOException {

   CheckpointStreamFactory.CheckpointStateOutputStream primaryOut =
      primaryStreamFactory.createCheckpointStateOutputStream(checkpointedStateScope);

   try {
      File outFile = new File(
         secondaryStreamDirProvider.subtaskSpecificCheckpointDirectory(checkpointId),
         String.valueOf(UUID.randomUUID()));
      Path outPath = new Path(outFile.toURI());

      CheckpointStreamFactory.CheckpointStateOutputStream secondaryOut =
         new FileBasedStateOutputStream(outPath.getFileSystem(), outPath);
//有两个输出流，primary 和 secondary，secondary 对应本地存储
      return new CheckpointStreamWithResultProvider.PrimaryAndSecondaryStream(primaryOut, secondaryOut);
   } catch (IOException secondaryEx) {
      LOG.warn("Exception when opening secondary/local checkpoint output stream. " +
         "Continue only with the primary stream.", secondaryEx);
   }

   return new CheckpointStreamWithResultProvider.PrimaryStreamOnly(primaryOut);
}
```

而RocksDBSnapshotStrategyBase的doSnapshot分为两种实现情况，RocksIncrementalSnapshotStrategy和RocksFullSnapshotStrategy分别对应增量和全量

```
public final RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
      long checkpointId,
      long timestamp,
      @Nonnull CheckpointStreamFactory streamFactory,
      @Nonnull CheckpointOptions checkpointOptions) throws Exception {

      if (kvStateInformation.isEmpty()) {
         if (LOG.isDebugEnabled()) {
            LOG.debug("Asynchronous RocksDB snapshot performed on empty keyed state at {}. Returning null.",
               timestamp);
         }
         return DoneFuture.of(SnapshotResult.empty());
      } else {
         return doSnapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
      }
   }

   /**
    * This method implements the concrete snapshot logic for a non-empty state.
    */
   @Nonnull
   protected abstract RunnableFuture<SnapshotResult<KeyedStateHandle>> doSnapshot(
      long checkpointId,
      long timestamp,
      CheckpointStreamFactory streamFactory,
      CheckpointOptions checkpointOptions) throws Exception;
}
```

至此，我们介绍了快照操作的第一个阶段，即同步执行的阶段。异步执行阶段被封装为 `AsyncCheckpointRunnable`，主要的操作包括 1）执行同步阶段创建的 FutureTask 2）完成后向 CheckpointCoordinator 发送 Ack 响应。

```
protected static final class AsyncCheckpointRunnable implements Runnable, Closeable {

   private final StreamTask<?, ?> owner;

   private final Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress;

   private final CheckpointMetaData checkpointMetaData;
   private final CheckpointMetrics checkpointMetrics;

   private final long asyncStartNanos;

   private final AtomicReference<CheckpointingOperation.AsyncCheckpointState> asyncCheckpointState = new AtomicReference<>(
      CheckpointingOperation.AsyncCheckpointState.RUNNING);

   @Override
   public void run() {
      FileSystemSafetyNet.initializeSafetyNetForThread();
      try {

         TaskStateSnapshot jobManagerTaskOperatorSubtaskStates =
            new TaskStateSnapshot(operatorSnapshotsInProgress.size());

         TaskStateSnapshot localTaskOperatorSubtaskStates =
            new TaskStateSnapshot(operatorSnapshotsInProgress.size());

// 完成每一个 operator 的状态写入
				// 如果是同步 checkpoint，那么在此之前状态已经写入完成
				// 如果是异步 checkpoint，那么在这里才会写入状态
         for (Map.Entry<OperatorID, OperatorSnapshotFutures> entry : operatorSnapshotsInProgress.entrySet()) {

            OperatorID operatorID = entry.getKey();
            OperatorSnapshotFutures snapshotInProgress = entry.getValue();

            // finalize the async part of all by executing all snapshot runnables
            OperatorSnapshotFinalizer finalizedSnapshots =
               new OperatorSnapshotFinalizer(snapshotInProgress);

            jobManagerTaskOperatorSubtaskStates.putSubtaskStateByOperatorID(
               operatorID,
               finalizedSnapshots.getJobManagerOwnedState());

            localTaskOperatorSubtaskStates.putSubtaskStateByOperatorID(
               operatorID,
               finalizedSnapshots.getTaskLocalState());
         }

         final long asyncEndNanos = System.nanoTime();
         final long asyncDurationMillis = (asyncEndNanos - asyncStartNanos) / 1_000_000L;

         checkpointMetrics.setAsyncDurationMillis(asyncDurationMillis);

         if (asyncCheckpointState.compareAndSet(CheckpointingOperation.AsyncCheckpointState.RUNNING,
            CheckpointingOperation.AsyncCheckpointState.COMPLETED)) {
//报告 snapshot 完成
            reportCompletedSnapshotStates(
               jobManagerTaskOperatorSubtaskStates,
               localTaskOperatorSubtaskStates,
               asyncDurationMillis);

         } else {
            LOG.debug("{} - asynchronous part of checkpoint {} could not be completed because it was closed before.",
               owner.getName(),
               checkpointMetaData.getCheckpointId());
         }
      } catch (Exception e) {
         if (LOG.isDebugEnabled()) {
            LOG.debug("{} - asynchronous part of checkpoint {} could not be completed.",
               owner.getName(),
               checkpointMetaData.getCheckpointId(),
               e);
         }
         handleExecutionException(e);
      } finally {
         owner.cancelables.unregisterCloseable(this);
         FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();
      }
      
      
private void reportCompletedSnapshotStates(
			TaskStateSnapshot acknowledgedTaskStateSnapshot,
			TaskStateSnapshot localTaskStateSnapshot,
			long asyncDurationMillis) {

			TaskStateManager taskStateManager = owner.getEnvironment().getTaskStateManager();

			boolean hasAckState = acknowledgedTaskStateSnapshot.hasState();
			boolean hasLocalState = localTaskStateSnapshot.hasState();

			Preconditions.checkState(hasAckState || !hasLocalState,
				"Found cached state but no corresponding primary state is reported to the job " +
					"manager. This indicates a problem.");

			// we signal stateless tasks by reporting null, so that there are no attempts to assign empty state
			// to stateless tasks on restore. This enables simple job modifications that only concern
			// stateless without the need to assign them uids to match their (always empty) states.
			taskStateManager.reportTaskStateSnapshots(
				checkpointMetaData,
				checkpointMetrics,
				hasAckState ? acknowledgedTaskStateSnapshot : null,
				hasLocalState ? localTaskStateSnapshot : null);

			LOG.debug("{} - finished asynchronous part of checkpoint {}. Asynchronous duration: {} ms",
				owner.getName(), checkpointMetaData.getCheckpointId(), asyncDurationMillis);

			LOG.trace("{} - reported the following states in snapshot for checkpoint {}: {}.",
				owner.getName(), checkpointMetaData.getCheckpointId(), acknowledgedTaskStateSnapshot);
		}
		
		TaskStateManager：
public void reportTaskStateSnapshots(
		@Nonnull CheckpointMetaData checkpointMetaData,
		@Nonnull CheckpointMetrics checkpointMetrics,
		@Nullable TaskStateSnapshot acknowledgedState,
		@Nullable TaskStateSnapshot localState) {

		long checkpointId = checkpointMetaData.getCheckpointId();

		localStateStore.storeLocalState(checkpointId, localState);
//发送 ACK 响应给 CheckpointCoordinator
		checkpointResponder.acknowledgeCheckpoint(
			jobId,
			executionAttemptID,
			checkpointId,
			checkpointMetrics,
			acknowledgedState);
	}
```

接下来进行Checkpoint 的确认，`Task` 对 checkpoint 的响应是通过 `CheckpointResponder` 接口完成的：

```
void acknowledgeCheckpoint(
   JobID jobID,
   ExecutionAttemptID executionAttemptID,
   long checkpointId,
   CheckpointMetrics checkpointMetrics,
   TaskStateSnapshot subtaskState);
```

`RpcCheckpointResponder` 作为 `CheckpointResponder` 的具体实现，主要是通过 RPC 调用通知 `CheckpointCoordinatorGateway`，即通知给 `JobMaster`, `JobMaster` 调用 `CheckpointCoordinator.receiveAcknowledgeMessage()` 和 `CheckpointCoordinator.receiveDeclineMessage()` 进行处理。

```
public void acknowledgeCheckpoint(
      JobID jobID,
      ExecutionAttemptID executionAttemptID,
      long checkpointId,
      CheckpointMetrics checkpointMetrics,
      TaskStateSnapshot subtaskState) {

   checkpointCoordinatorGateway.acknowledgeCheckpoint(
      jobID,
      executionAttemptID,
      checkpointId,
      checkpointMetrics,
      subtaskState);
}
```

JobMaster

```
public void acknowledgeCheckpoint(
      final JobID jobID,
      final ExecutionAttemptID executionAttemptID,
      final long checkpointId,
      final CheckpointMetrics checkpointMetrics,
      final TaskStateSnapshot checkpointState) {

   schedulerNG.acknowledgeCheckpoint(jobID, executionAttemptID, checkpointId, checkpointMetrics, checkpointState);
}
```

SchedulerBase实现SchedulerNG接口：

```
public void acknowledgeCheckpoint(final JobID jobID, final ExecutionAttemptID executionAttemptID, final long checkpointId, final CheckpointMetrics checkpointMetrics, final TaskStateSnapshot checkpointState) {
   mainThreadExecutor.assertRunningInMainThread();

//获取CheckpointCoordinator
   final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
   //Checkpoint的确定Message
   final AcknowledgeCheckpoint ackMessage = new AcknowledgeCheckpoint(
      jobID,
      executionAttemptID,
      checkpointId,
      checkpointMetrics,
      checkpointState);

//获取发送checkpoint完成信息的TaskManager的Location
   final String taskManagerLocationInfo = retrieveTaskManagerLocation(executionAttemptID);

   if (checkpointCoordinator != null) {
      ioExecutor.execute(() -> {
         try {
            checkpointCoordinator.receiveAcknowledgeMessage(ackMessage, taskManagerLocationInfo);
         } catch (Throwable t) {
            log.warn("Error while processing checkpoint acknowledgement message", t);
         }
      });
   } else {
      String errorMessage = "Received AcknowledgeCheckpoint message for job {} with no CheckpointCoordinator";
      if (executionGraph.getState() == JobStatus.RUNNING) {
         log.error(errorMessage, jobGraph.getJobID());
      } else {
         log.debug(errorMessage, jobGraph.getJobID());
      }
   }
}
```

CheckpointCoordinator的receiveAcknowledgeMessage接收来自Task的确认消息

在一个 `Task` 完成 checkpoint 操作后，`CheckpointCoordinator` 接收到 Ack 响应，对 Ack 响应的处理流程主要如下：

- 根据 Ack 的 checkpointID 从 `Map pendingCheckpoints` 中查找对应的 `PendingCheckpoint`

- 若存在对应的

   

  ```
  PendingCheckpoint
  ```

  - 这个

     

    ```
    PendingCheckpoint
    ```

     

    没有被丢弃，调用

     

    ```
    PendingCheckpoint.acknowledgeTask
    ```

     

    方法处理 Ack，根据处理结果的不同：

    - SUCCESS：判断是否已经接受了所有需要响应的 Ack，如果是，则调用 `completePendingCheckpoint` 完成此次 checkpoint
    - DUPLICATE：Ack 消息重复接收，直接忽略
    - UNKNOWN：未知的 Ack 消息，清理上报的 Ack 中携带的状态句柄
    - DISCARD：Checkpoint 已经被 discard，清理上报的 Ack 中携带的状态句柄

  - 这个 `PendingCheckpoint` 已经被丢弃，抛出异常

- 若不存在对应的 `PendingCheckpoint`，则清理上报的 Ack 中携带的状态句柄

```
public boolean receiveAcknowledgeMessage(AcknowledgeCheckpoint message, String taskManagerLocationInfo) throws CheckpointException {
   if (shutdown || message == null) {
      return false;
   }

   if (!job.equals(message.getJob())) {
      LOG.error("Received wrong AcknowledgeCheckpoint message for job {} from {} : {}", job, taskManagerLocationInfo, message);
      return false;
   }

   final long checkpointId = message.getCheckpointId();

   synchronized (lock) {
      // we need to check inside the lock for being shutdown as well, otherwise we
      // get races and invalid error log messages
      if (shutdown) {
         return false;
      }

      final PendingCheckpoint checkpoint = pendingCheckpoints.get(checkpointId);

      if (checkpoint != null && !checkpoint.isDiscarded()) {

         switch (checkpoint.acknowledgeTask(message.getTaskExecutionId(), message.getSubtaskState(), message.getCheckpointMetrics())) {
            case SUCCESS:
               LOG.debug("Received acknowledge message for checkpoint {} from task {} of job {} at {}.",
                  checkpointId, message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);

               if (checkpoint.areTasksFullyAcknowledged()) {
                  completePendingCheckpoint(checkpoint);
               }
               break;
            case DUPLICATE:
               LOG.debug("Received a duplicate acknowledge message for checkpoint {}, task {}, job {}, location {}.",
                  message.getCheckpointId(), message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);
               break;
            case UNKNOWN:
               LOG.warn("Could not acknowledge the checkpoint {} for task {} of job {} at {}, " +
                     "because the task's execution attempt id was unknown. Discarding " +
                     "the state handle to avoid lingering state.", message.getCheckpointId(),
                  message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);

               discardSubtaskState(message.getJob(), message.getTaskExecutionId(), message.getCheckpointId(), message.getSubtaskState());

               break;
            case DISCARDED:
               LOG.warn("Could not acknowledge the checkpoint {} for task {} of job {} at {}, " +
                  "because the pending checkpoint had been discarded. Discarding the " +
                     "state handle tp avoid lingering state.",
                  message.getCheckpointId(), message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);

               discardSubtaskState(message.getJob(), message.getTaskExecutionId(), message.getCheckpointId(), message.getSubtaskState());
         }

         return true;
      }
      else if (checkpoint != null) {
         // this should not happen
         throw new IllegalStateException(
               "Received message for discarded but non-removed checkpoint " + checkpointId);
      }
      else {
         boolean wasPendingCheckpoint;

         // message is for an unknown checkpoint, or comes too late (checkpoint disposed)
         if (recentPendingCheckpoints.contains(checkpointId)) {
            wasPendingCheckpoint = true;
            LOG.warn("Received late message for now expired checkpoint attempt {} from task " +
               "{} of job {} at {}.", checkpointId, message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);
         }
         else {
            LOG.debug("Received message for an unknown checkpoint {} from task {} of job {} at {}.",
               checkpointId, message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);
            wasPendingCheckpoint = false;
         }

         // try to discard the state so that we don't have lingering state lying around
         discardSubtaskState(message.getJob(), message.getTaskExecutionId(), message.getCheckpointId(), message.getSubtaskState());

         return wasPendingCheckpoint;
      }
   }
   
```



一旦 `PendingCheckpoint` 确认所有 Ack 消息都已经接收，那么就可以完成此次 checkpoint 了，具体包括：

- 调用

  ```
  PendingCheckpoint.finalizeCheckpoint()
  ```

  将

  ```
  PendingCheckpoint
  ```

  转化为

  ```
  CompletedCheckpoint
  ```

  - 获取 `CheckpointMetadataOutputStream`，将所有的状态句柄信息通过 `CheckpointMetadataOutputStream` 写入到存储系统中
  - 创建一个 `CompletedCheckpoint` 对象

- 将

   

  ```
  CompletedCheckpoint
  ```

   

  保存到

   

  ```
  CompletedCheckpointStore
  ```

   

  中

  - `CompletedCheckpointStore` 有两种实现，分别为 `StandaloneCompletedCheckpointStore` 和 `ZooKeeperCompletedCheckpointStore`
  - `StandaloneCompletedCheckpointStore` 简单地将 `CompletedCheckpointStore` 存放在一个数组中
  - `ZooKeeperCompletedCheckpointStore` 提供高可用实现：先将 `CompletedCheckpointStore` 写入到 `RetrievableStateStorageHelper` 中（通常是文件系统），然后将文件句柄存在 ZK 中
  - 保存的 `CompletedCheckpointStore` 数量是有限的，会删除旧的快照

- 移除被越过的 `PendingCheckpoint`，因为 `CheckpointID` 是递增的，那么所有比当前完成的 `CheckpointID` 小的 `PendingCheckpoint` 都可以被丢弃了

- 依次调用

   

  ```
  Execution.notifyCheckpointComplete()
  ```

   

  通知所有的 Task 当前 Checkpoint 已经完成，在这一步会向kafka提交offset信息

  - 通过 RPC 调用 `TaskExecutor.confirmCheckpoint()` 告知对应的 Task

```
private void completePendingCheckpoint(PendingCheckpoint pendingCheckpoint) throws CheckpointException {
   final long checkpointId = pendingCheckpoint.getCheckpointId();
   final CompletedCheckpoint completedCheckpoint;

   // As a first step to complete the checkpoint, we register its state with the registry
   Map<OperatorID, OperatorState> operatorStates = pendingCheckpoint.getOperatorStates();
   sharedStateRegistry.registerAll(operatorStates.values());

   try {
      try {
         completedCheckpoint = pendingCheckpoint.finalizeCheckpoint();
         failureManager.handleCheckpointSuccess(pendingCheckpoint.getCheckpointId());
      }
      catch (Exception e1) {
         // abort the current pending checkpoint if we fails to finalize the pending checkpoint.
         if (!pendingCheckpoint.isDiscarded()) {
            failPendingCheckpoint(pendingCheckpoint, CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE, e1);
         }

         throw new CheckpointException("Could not finalize the pending checkpoint " + checkpointId + '.',
            CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE, e1);
      }

      // the pending checkpoint must be discarded after the finalization
      Preconditions.checkState(pendingCheckpoint.isDiscarded() && completedCheckpoint != null);

      try {
         completedCheckpointStore.addCheckpoint(completedCheckpoint);
      } catch (Exception exception) {
         // we failed to store the completed checkpoint. Let's clean up
         executor.execute(new Runnable() {
            @Override
            public void run() {
               try {
                  completedCheckpoint.discardOnFailedStoring();
               } catch (Throwable t) {
                  LOG.warn("Could not properly discard completed checkpoint {}.", completedCheckpoint.getCheckpointID(), t);
               }
            }
         });

         throw new CheckpointException("Could not complete the pending checkpoint " + checkpointId + '.',
            CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE, exception);
      }
   } finally {
      pendingCheckpoints.remove(checkpointId);

      triggerQueuedRequests();
   }

   rememberRecentCheckpointId(checkpointId);

   // drop those pending checkpoints that are at prior to the completed one
   dropSubsumedCheckpoints(checkpointId);

   // record the time when this was completed, to calculate
   // the 'min delay between checkpoints'
   lastCheckpointCompletionRelativeTime = clock.relativeTimeMillis();

   LOG.info("Completed checkpoint {} for job {} ({} bytes in {} ms).", checkpointId, job,
      completedCheckpoint.getStateSize(), completedCheckpoint.getDuration());

   if (LOG.isDebugEnabled()) {
      StringBuilder builder = new StringBuilder();
      builder.append("Checkpoint state: ");
      for (OperatorState state : completedCheckpoint.getOperatorStates().values()) {
         builder.append(state);
         builder.append(", ");
      }
      // Remove last two chars ", "
      builder.setLength(builder.length() - 2);

      LOG.debug(builder.toString());
   }

   // send the "notify complete" call to all vertices
   final long timestamp = completedCheckpoint.getTimestamp();

   for (ExecutionVertex ev : tasksToCommitTo) {
      Execution ee = ev.getCurrentExecutionAttempt();
      if (ee != null) {
         ee.notifyCheckpointComplete(checkpointId, timestamp);
      }
   }
}
```

对于一个已经触发但还没有完成的 checkpoint，即 `PendingCheckpoint`，它是如何处理 Ack 消息的呢？在 `PendingCheckpoint` 内部维护了两个 Map，分别是：

- `Map operatorStates;` : 已经接收到 Ack 的算子的状态句柄
- `Map notYetAcknowledgedTasks;`: 需要 Ack 但还没有接收到的 Task

每当接收到一个 Ack 消息时，`PendingCheckpoint` 就从 `notYetAcknowledgedTasks` 中移除对应的 Task，并保存 Ack 携带的状态句柄保存。当 `notYetAcknowledgedTasks` 为空时，表明所有的 Ack 消息都接收到了。



```
public class PendingCheckpoint {

   /**
    * Result of the {@link PendingCheckpoint#acknowledgedTasks} method.
    */
   public enum TaskAcknowledgeResult {
      SUCCESS, // successful acknowledge of the task
      DUPLICATE, // acknowledge message is a duplicate
      UNKNOWN, // unknown task acknowledged
      DISCARDED // pending checkpoint has been discarded
   }

   // ------------------------------------------------------------------------

   /** The PendingCheckpoint logs to the same logger as the CheckpointCoordinator. */
   private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinator.class);

   private final Object lock = new Object();

   private final JobID jobId;

   private final long checkpointId;

   private final long checkpointTimestamp;

   private final Map<OperatorID, OperatorState> operatorStates;

   private final Map<ExecutionAttemptID, ExecutionVertex> notYetAcknowledgedTasks;

   private final List<MasterState> masterStates;

   private final Set<String> notYetAcknowledgedMasterStates;

   /** Set of acknowledged tasks. */
   private final Set<ExecutionAttemptID> acknowledgedTasks;
```

其中 `OperatorState` 是算子状态句柄的一层封装：

```
public class OperatorState implements CompositeStateHandle {

   private static final long serialVersionUID = -4845578005863201810L;

   /** id of the operator */
   private final OperatorID operatorID;

   /** handles to non-partitioned states, subtaskindex -> subtaskstate */
   private final Map<Integer, OperatorSubtaskState> operatorSubtaskStates;

   /** parallelism of the operator when it was checkpointed */
   private final int parallelism;

   /** maximum parallelism of the operator when the job was first created */
   private final int maxParallelism;
```

```
public class OperatorSubtaskState implements CompositeStateHandle {

   private static final Logger LOG = LoggerFactory.getLogger(OperatorSubtaskState.class);

   private static final long serialVersionUID = -2394696997971923995L;

   /**
    * Snapshot from the {@link org.apache.flink.runtime.state.OperatorStateBackend}.
    */
   @Nonnull
   private final StateObjectCollection<OperatorStateHandle> managedOperatorState;

   /**
    * Snapshot written using {@link org.apache.flink.runtime.state.OperatorStateCheckpointOutputStream}.
    */
   @Nonnull
   private final StateObjectCollection<OperatorStateHandle> rawOperatorState;

   /**
    * Snapshot from {@link org.apache.flink.runtime.state.KeyedStateBackend}.
    */
   @Nonnull
   private final StateObjectCollection<KeyedStateHandle> managedKeyedState;

   /**
    * Snapshot written using {@link org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream}.
    */
   @Nonnull
   private final StateObjectCollection<KeyedStateHandle> rawKeyedState;
```

在 Task 进行 checkpoint 的过程，可能会发生异常导致 checkpoint 失败，在这种情况下会通过 `CheckpointResponder` 发出回绝的消息。当 `CheckpointCoordinator` 接收到 `DeclineCheckpoint` 消息后会移除 `PendingCheckpoint`，并尝试丢弃已经接收到的 Ack 消息中已完成的状态句柄：

```
public void receiveDeclineMessage(DeclineCheckpoint message, String taskManagerLocationInfo) {
   if (shutdown || message == null) {
      return;
   }

   if (!job.equals(message.getJob())) {
      throw new IllegalArgumentException("Received DeclineCheckpoint message for job " +
         message.getJob() + " from " + taskManagerLocationInfo + " while this coordinator handles job " + job);
   }

   final long checkpointId = message.getCheckpointId();
   final String reason = (message.getReason() != null ? message.getReason().getMessage() : "");

   PendingCheckpoint checkpoint;

   synchronized (lock) {
      // we need to check inside the lock for being shutdown as well, otherwise we
      // get races and invalid error log messages
      if (shutdown) {
         return;
      }

      checkpoint = pendingCheckpoints.remove(checkpointId);

      if (checkpoint != null && !checkpoint.isDiscarded()) {
         LOG.info("Decline checkpoint {} by task {} of job {} at {}.",
            checkpointId,
            message.getTaskExecutionId(),
            job,
            taskManagerLocationInfo);
         discardCheckpoint(checkpoint, message.getReason(), message.getTaskExecutionId());
      }
      else if (checkpoint != null) {
         // this should not happen
         throw new IllegalStateException(
               "Received message for discarded but non-removed checkpoint " + checkpointId);
      }
      else if (LOG.isDebugEnabled()) {
         if (recentPendingCheckpoints.contains(checkpointId)) {
            // message is for an unknown checkpoint, or comes too late (checkpoint disposed)
            LOG.debug("Received another decline message for now expired checkpoint attempt {} from task {} of job {} at {} : {}",
                  checkpointId, message.getTaskExecutionId(), job, taskManagerLocationInfo, reason);
         } else {
            // message is for an unknown checkpoint. might be so old that we don't even remember it any more
            LOG.debug("Received decline message for unknown (too old?) checkpoint attempt {} from task {} of job {} at {} : {}",
                  checkpointId, message.getTaskExecutionId(), job, taskManagerLocationInfo, reason);
         }
      }
   }
}
```

当 Flink 作业失败重启或者从指定 SavePoint 启动时，需要将整个作业恢复到上一次成功 checkpoint 的状态。这里主要分为两个阶段：

- `CheckpointCoordinator` 加载最近一次成功的 `CompletedCheckpoint`，并将状态重新分配到不同的 `Execution`（`Task`）中
- `Task` 启动时进行状态初始化

首先，`JobMaster` 在创建 `ExecutionGraph` 后会尝试恢复状态到最近一次成功的 checkpoint，或者加载 SavePoint，最终都会调用 `CheckpointCoordinator.restoreLatestCheckpointedState()` 方法：

```
JobMaster:
private SchedulerNG createScheduler(final JobManagerJobMetricGroup jobManagerJobMetricGroup) throws Exception {
   return schedulerNGFactory.createInstance(
      log,
      jobGraph,
      backPressureStatsTracker,
      scheduledExecutorService,
      jobMasterConfiguration.getConfiguration(),
      scheduler,
      scheduledExecutorService,
      userCodeLoader,
      highAvailabilityServices.getCheckpointRecoveryFactory(),
      rpcTimeout,
      blobWriter,
      jobManagerJobMetricGroup,
      jobMasterConfiguration.getSlotRequestTimeout(),
      shuffleMaster,
      partitionTracker);
}


SchedulerBase:
private ExecutionGraph createAndRestoreExecutionGraph(
		JobManagerJobMetricGroup currentJobManagerJobMetricGroup,
		ShuffleMaster<?> shuffleMaster,
		JobMasterPartitionTracker partitionTracker) throws Exception {

		ExecutionGraph newExecutionGraph = createExecutionGraph(currentJobManagerJobMetricGroup, shuffleMaster, partitionTracker);

		final CheckpointCoordinator checkpointCoordinator = newExecutionGraph.getCheckpointCoordinator();

		if (checkpointCoordinator != null) {
			// check whether we find a valid checkpoint
			if (!checkpointCoordinator.restoreLatestCheckpointedState(
				new HashSet<>(newExecutionGraph.getAllVertices().values()),
				false,
				false)) {

				// check whether we can restore from a savepoint
				tryRestoreExecutionGraphFromSavepoint(newExecutionGraph, jobGraph.getSavepointRestoreSettings());
			}
		}

		return newExecutionGraph;
	}
```

CheckpointCoordinator.restoreLatestCheckpointedState()

```
public boolean restoreLatestCheckpointedState(
      final Set<ExecutionJobVertex> tasks,
      final boolean errorIfNoCheckpoint,
      final boolean allowNonRestoredState) throws Exception {

   synchronized (lock) {
      if (shutdown) {
         throw new IllegalStateException("CheckpointCoordinator is shut down");
      }

      // We create a new shared state registry object, so that all pending async disposal requests from previous
      // runs will go against the old object (were they can do no harm).
      // This must happen under the checkpoint lock.
      sharedStateRegistry.close();
      sharedStateRegistry = sharedStateRegistryFactory.create(executor);

      // Recover the checkpoints, TODO this could be done only when there is a new leader, not on each recovery
      completedCheckpointStore.recover();

      // Now, we re-register all (shared) states from the checkpoint store with the new registry
      for (CompletedCheckpoint completedCheckpoint : completedCheckpointStore.getAllCheckpoints()) {
         completedCheckpoint.registerSharedStatesAfterRestored(sharedStateRegistry);
      }

      LOG.debug("Status of the shared state registry of job {} after restore: {}.", job, sharedStateRegistry);

      // Restore from the latest checkpoint
      CompletedCheckpoint latest = completedCheckpointStore.getLatestCheckpoint(isPreferCheckpointForRecovery);

      if (latest == null) {
         if (errorIfNoCheckpoint) {
            throw new IllegalStateException("No completed checkpoint available");
         } else {
            LOG.debug("Resetting the master hooks.");
            MasterHooks.reset(masterHooks.values(), LOG);

            return false;
         }
      }

      LOG.info("Restoring job {} from latest valid checkpoint: {}.", job, latest);

      // re-assign the task states
      final Map<OperatorID, OperatorState> operatorStates = latest.getOperatorStates();

      StateAssignmentOperation stateAssignmentOperation =
            new StateAssignmentOperation(latest.getCheckpointID(), tasks, operatorStates, allowNonRestoredState);

      stateAssignmentOperation.assignStates();

      // call master hooks for restore

      MasterHooks.restoreMasterHooks(
            masterHooks,
            latest.getMasterHookStates(),
            latest.getCheckpointID(),
            allowNonRestoredState,
            LOG);

      // update metrics

      if (statsTracker != null) {
         long restoreTimestamp = System.currentTimeMillis();
         RestoredCheckpointStats restored = new RestoredCheckpointStats(
            latest.getCheckpointID(),
            latest.getProperties(),
            restoreTimestamp,
            latest.getExternalPointer());

         statsTracker.reportRestoredCheckpoint(restored);
      }

      return true;
   }
}
```

状态的分配过程被封装在 `StateAssignmentOperation` 中。在状态恢复的过程中，假如任务的并行度发生变化，那么每个子任务的状态和先前必然是不一致的，这其中就涉及到状态的平均重新分配问题，关于状态分配的细节，可以参考 Flink 团队的博文 [A Deep Dive into Rescalable State in Apache Flink](https://flink.apache.org/features/2017/07/04/flink-rescalable-state.html#reassigning-operator-state-when-rescaling)，里面给出了 operator state 和 keyed state 重新分配的详细介绍。

最终，每个 `Task` 分配的状态被封装在 `JobManagerTaskRestore` 中，并通过 `Execution.setInitialState()` 关联到 `Execution` 中。`JobManagerTaskRestore` 回作为 `TaskDeploymentDescriptor` 的一个属性下发到 `TaskExecutor` 中。

当 `TaskDeploymentDescriptor` 被提交给 `TaskExecutor` 之后，`TaskExecutor` 会 `TaskStateManager` 用于管理当前 `Task` 的状态，`TaskStateManager` 对象会基于分配的 `JobManagerTaskRestore` 和本地状态存储 `TaskLocalStateStore` 进行创建：

```
public CompletableFuture<Acknowledge> submitTask(
      TaskDeploymentDescriptor tdd,
      JobMasterId jobMasterId,
      Time timeout) {
......
   //本地状态存储
      final TaskLocalStateStore localStateStore = localStateStoresManager.localStateStoreForSubtask(
         jobId,
         tdd.getAllocationId(),
         taskInformation.getJobVertexId(),
         tdd.getSubtaskIndex());
//由 JobManager 分配的用于恢复的状态
      final JobManagerTaskRestore taskRestore = tdd.getTaskRestore();
//创建 TaskStateManager
      final TaskStateManager taskStateManager = new TaskStateManagerImpl(
         jobId,
         tdd.getExecutionAttemptId(),
         localStateStore,
         taskRestore,
         checkpointResponder);

      ......
}
```

在 `Task` 启动后，`StreamTask` 会先调用 `initializeState` 方法，这样每一个算子都会调用 `StreamOperator.initializeState()` 进行状态的初始化：

```
StreamTask:
private void initializeStateAndOpen() throws Exception {

   StreamOperator<?>[] allOperators = operatorChain.getAllOperators();

   for (StreamOperator<?> operator : allOperators) {
      if (null != operator) {
         operator.initializeState();
         operator.open();
      }
   }
}

AbstractStreamOperator:
public final void initializeState() throws Exception {

		final TypeSerializer<?> keySerializer = config.getStateKeySerializer(getUserCodeClassloader());

		final StreamTask<?, ?> containingTask =
			Preconditions.checkNotNull(getContainingTask());
		final CloseableRegistry streamTaskCloseableRegistry =
			Preconditions.checkNotNull(containingTask.getCancelables());
		final StreamTaskStateInitializer streamTaskStateManager =
			Preconditions.checkNotNull(containingTask.createStreamTaskStateInitializer());

//创建 StreamOperatorStateContext，这一步会进行状态的恢复，
		//这样 operatorStateBackend 和 keyedStateBackend 就可以恢复到到最后一次 checkpoint 的状态
		//timeServiceManager 也会恢复
		final StreamOperatorStateContext context =
			streamTaskStateManager.streamOperatorStateContext(
				getOperatorID(),
				getClass().getSimpleName(),
				getProcessingTimeService(),
				this,
				keySerializer,
				streamTaskCloseableRegistry,
				metrics);

		this.operatorStateBackend = context.operatorStateBackend();
		this.keyedStateBackend = context.keyedStateBackend();

		if (keyedStateBackend != null) {
			this.keyedStateStore = new DefaultKeyedStateStore(keyedStateBackend, getExecutionConfig());
		}

		timeServiceManager = context.internalTimerServiceManager();

		CloseableIterable<KeyGroupStatePartitionStreamProvider> keyedStateInputs = context.rawKeyedStateInputs();
		CloseableIterable<StatePartitionStreamProvider> operatorStateInputs = context.rawOperatorStateInputs();

		try {
		//StateInitializationContext 对外暴露了 state backend，timer service manager 等，operator 可以借助它来进行状态初始化
			StateInitializationContext initializationContext = new StateInitializationContextImpl(
				context.isRestored(), // information whether we restore or start for the first time
				operatorStateBackend, // access to operator state backend
				keyedStateStore, // access to keyed state backend
				keyedStateInputs, // access to keyed state stream
				operatorStateInputs); // access to operator state stream

//进行状态初始化，在子类中实现，比如调用 UDF 的状态初始化方法
			initializeState(initializationContext);
		} finally {
			closeFromRegistry(operatorStateInputs, streamTaskCloseableRegistry);
			closeFromRegistry(keyedStateInputs, streamTaskCloseableRegistry);
		}
	}
	
	
public void initializeState(StateInitializationContext context) throws Exception {

	}
	
	
AbstractUdfStreamOperator:
public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		StreamingFunctionUtils.restoreFunctionState(context, userFunction);
	}
```

状态恢复的关键操作在于通过 `StreamTaskStateInitializer.streamOperatorStateContext()` 生成 `StreamOperatorStateContext`, 通过 `StreamOperatorStateContext` 可以获取 state backend，timer service manager 等：

streamTaskStateManager.streamOperatorStateContext

```
StreamTaskStateInitializer:
StreamOperatorStateContext streamOperatorStateContext(
   @Nonnull OperatorID operatorID,
   @Nonnull String operatorClassName,
   @Nonnull ProcessingTimeService processingTimeService,
   @Nonnull KeyContext keyContext,
   @Nullable TypeSerializer<?> keySerializer,
   @Nonnull CloseableRegistry streamTaskCloseableRegistry,
   @Nonnull MetricGroup metricGroup) throws Exception;
   
  
 
 public interface StreamOperatorStateContext {

	/**
	 * Returns true, the states provided by this context are restored from a checkpoint/savepoint.
	 */
	boolean isRestored();

	/**
	 * Returns the operator state backend for the stream operator.
	 */
	OperatorStateBackend operatorStateBackend();

	/**
	 * Returns the keyed state backend for the stream operator. This method returns null for non-keyed operators.
	 */
	AbstractKeyedStateBackend<?> keyedStateBackend();

	/**
	 * Returns the internal timer service manager for the stream operator. This method returns null for non-keyed
	 * operators.
	 */
	InternalTimeServiceManager<?> internalTimerServiceManager();

	/**
	 * Returns an iterable to obtain input streams for previously stored operator state partitions that are assigned to
	 * this stream operator.
	 */
	CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs();

	/**
	 * Returns an iterable to obtain input streams for previously stored keyed state partitions that are assigned to
	 * this operator. This method returns null for non-keyed operators.
	 */
	CloseableIterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs();

}

```

StreamTaskStateInitializerImpl实现了StreamTaskStateInitializer接口

```
public StreamOperatorStateContext streamOperatorStateContext(
   @Nonnull OperatorID operatorID,
   @Nonnull String operatorClassName,
   @Nonnull ProcessingTimeService processingTimeService,
   @Nonnull KeyContext keyContext,
   @Nullable TypeSerializer<?> keySerializer,
   @Nonnull CloseableRegistry streamTaskCloseableRegistry,
   @Nonnull MetricGroup metricGroup) throws Exception {

   TaskInfo taskInfo = environment.getTaskInfo();
   OperatorSubtaskDescriptionText operatorSubtaskDescription =
      new OperatorSubtaskDescriptionText(
         operatorID,
         operatorClassName,
         taskInfo.getIndexOfThisSubtask(),
         taskInfo.getNumberOfParallelSubtasks());

   final String operatorIdentifierText = operatorSubtaskDescription.toString();

//先获取用于恢复状态的 PrioritizedOperatorSubtaskState
   final PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskStates =
      taskStateManager.prioritizedOperatorState(operatorID);

   AbstractKeyedStateBackend<?> keyedStatedBackend = null;
   OperatorStateBackend operatorStateBackend = null;
   CloseableIterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs = null;
   CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs = null;
   InternalTimeServiceManager<?> timeServiceManager;

   try {

      // -------------- Keyed State Backend --------------
      keyedStatedBackend = keyedStatedBackend(
         keySerializer,
         operatorIdentifierText,
         prioritizedOperatorSubtaskStates,
         streamTaskCloseableRegistry,
         metricGroup);

      // -------------- Operator State Backend --------------
      operatorStateBackend = operatorStateBackend(
         operatorIdentifierText,
         prioritizedOperatorSubtaskStates,
         streamTaskCloseableRegistry);

      // -------------- Raw State Streams --------------
      rawKeyedStateInputs = rawKeyedStateInputs(
         prioritizedOperatorSubtaskStates.getPrioritizedRawKeyedState().iterator());
      streamTaskCloseableRegistry.registerCloseable(rawKeyedStateInputs);

      rawOperatorStateInputs = rawOperatorStateInputs(
         prioritizedOperatorSubtaskStates.getPrioritizedRawOperatorState().iterator());
      streamTaskCloseableRegistry.registerCloseable(rawOperatorStateInputs);

      // -------------- Internal Timer Service Manager --------------
      timeServiceManager = internalTimeServiceManager(keyedStatedBackend, keyContext, processingTimeService, rawKeyedStateInputs);

      // -------------- Preparing return value --------------

      return new StreamOperatorStateContextImpl(
         prioritizedOperatorSubtaskStates.isRestored(),
         operatorStateBackend,
         keyedStatedBackend,
         timeServiceManager,
         rawOperatorStateInputs,
         rawKeyedStateInputs);
   } 
}
```

状态恢复和创建创建 state backend 耦合在一起，借助 `BackendRestorerProcedure` 来完成，具体的逻辑在 `BackendRestorerProcedure.createAndRestore` 方法中。

以operatorStateBackend为例：

```
protected OperatorStateBackend operatorStateBackend(
   String operatorIdentifierText,
   PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskStates,
   CloseableRegistry backendCloseableRegistry) throws Exception {

   String logDescription = "operator state backend for " + operatorIdentifierText;

   // Now restore processing is included in backend building/constructing process, so we need to make sure
   // each stream constructed in restore could also be closed in case of task cancel, for example the data
   // input stream opened for serDe during restore.
   CloseableRegistry cancelStreamRegistryForRestore = new CloseableRegistry();
   backendCloseableRegistry.registerCloseable(cancelStreamRegistryForRestore);
   BackendRestorerProcedure<OperatorStateBackend, OperatorStateHandle> backendRestorer =
      new BackendRestorerProcedure<>(
         (stateHandles) -> stateBackend.createOperatorStateBackend(
            environment,
            operatorIdentifierText,
            stateHandles,
            cancelStreamRegistryForRestore),
         backendCloseableRegistry,
         logDescription);

   try {
   //创建及恢复状态
      return backendRestorer.createAndRestore(
         prioritizedOperatorSubtaskStates.getPrioritizedManagedOperatorState());
   } finally {
      if (backendCloseableRegistry.unregisterCloseable(cancelStreamRegistryForRestore)) {
         IOUtils.closeQuietly(cancelStreamRegistryForRestore);
      }
   }
}
```

BackendRestorerProcedure的createAndRestore

```
public T createAndRestore(@Nonnull List<? extends Collection<S>> restoreOptions) throws Exception {

   if (restoreOptions.isEmpty()) {
      restoreOptions = Collections.singletonList(Collections.emptyList());
   }

   int alternativeIdx = 0;

   Exception collectedException = null;

   while (alternativeIdx < restoreOptions.size()) {

      Collection<S> restoreState = restoreOptions.get(alternativeIdx);

      ++alternativeIdx;

      // IMPORTANT: please be careful when modifying the log statements because they are used for validation in
      // the automatic end-to-end tests. Those tests might fail if they are not aligned with the log message!
      if (restoreState.isEmpty()) {
         LOG.debug("Creating {} with empty state.", logDescription);
      } else {
         if (LOG.isTraceEnabled()) {
            LOG.trace("Creating {} and restoring with state {} from alternative ({}/{}).",
               logDescription, restoreState, alternativeIdx, restoreOptions.size());
         } else {
            LOG.debug("Creating {} and restoring with state from alternative ({}/{}).",
               logDescription, alternativeIdx, restoreOptions.size());
         }
      }

      try {
         return attemptCreateAndRestore(restoreState);
      } catch (Exception ex) {

         collectedException = ExceptionUtils.firstOrSuppressed(ex, collectedException);

         LOG.warn("Exception while restoring {} from alternative ({}/{}), will retry while more " +
            "alternatives are available.", logDescription, alternativeIdx, restoreOptions.size(), ex);

         if (backendCloseableRegistry.isClosed()) {
            throw new FlinkException("Stopping restore attempts for already cancelled task.", collectedException);
         }
      }
   }

   throw new FlinkException("Could not restore " + logDescription + " from any of the " + restoreOptions.size() +
      " provided restore options.", collectedException);
}

private T attemptCreateAndRestore(Collection<S> restoreState) throws Exception {

   // create a new backend with necessary initialization.
   //创建及恢复状态
   //instanceSupplier有具体状态后端实现，就是StateBackend接口的createOperatorStateBackend方法
   final T backendInstance = instanceSupplier.apply(restoreState);

   try {
      // register the backend with the registry to participate in task lifecycle w.r.t. cancellation.
      backendCloseableRegistry.registerCloseable(backendInstance);
      return backendInstance;
   } catch (Exception ex) {
      // dispose the backend, e.g. to release native resources, if failed to register it into registry.
      try {
         backendInstance.dispose();
      } catch (Exception disposeEx) {
         ex = ExceptionUtils.firstOrSuppressed(disposeEx, ex);
      }

      throw ex;
   }
}
```

TaskStateManagerImpl的prioritizedOperatorState：

```
public PrioritizedOperatorSubtaskState prioritizedOperatorState(OperatorID operatorID) {

   if (jobManagerTaskRestore == null) {
      return PrioritizedOperatorSubtaskState.emptyNotRestored();
   }
//从 JobManager 获取的状态快照
   TaskStateSnapshot jobManagerStateSnapshot =
      jobManagerTaskRestore.getTaskStateSnapshot();

   OperatorSubtaskState jobManagerSubtaskState =
      jobManagerStateSnapshot.getSubtaskStateByOperatorID(operatorID);

   if (jobManagerSubtaskState == null) {
      return PrioritizedOperatorSubtaskState.emptyNotRestored();
   }
//本地状态快照作为备选
   long restoreCheckpointId = jobManagerTaskRestore.getRestoreCheckpointId();

   TaskStateSnapshot localStateSnapshot =
      localStateStore.retrieveLocalState(restoreCheckpointId);

   localStateStore.pruneMatchingCheckpoints((long checkpointId) -> checkpointId != restoreCheckpointId);

   List<OperatorSubtaskState> alternativesByPriority = Collections.emptyList();

   if (localStateSnapshot != null) {
      OperatorSubtaskState localSubtaskState = localStateSnapshot.getSubtaskStateByOperatorID(operatorID);

      if (localSubtaskState != null) {
         alternativesByPriority = Collections.singletonList(localSubtaskState);
      }
   }

   LOG.debug("Operator {} has remote state {} from job manager and local state alternatives {} from local " +
         "state store {}.", operatorID, jobManagerSubtaskState, alternativesByPriority, localStateStore);
//构建 PrioritizedOperatorSubtaskState
   PrioritizedOperatorSubtaskState.Builder builder = new PrioritizedOperatorSubtaskState.Builder(
      jobManagerSubtaskState,
      alternativesByPriority,
      true);

   return builder.build();
}
```

为了生成 `StreamOperatorStateContext`，首先要通过 `TaskStateManager.prioritizedOperatorState()` 方法获得每个 Operator 需要恢复的状态句柄；然后使用获得的状态句柄创建并还原 state backend 和 timer。这里引入了 `PrioritizedOperatorSubtaskState`, 它封装了多个备选的 OperatorSubtaskState (快照)，这些快照相互之间是可以（部分）替换的，并按照优先级排序。列表中的最后一项是包含了这个子任务的所有状态，但是优先级最低。在进行状态恢复的时候，优先从高优先级的状态句柄中读取状态。

PrioritizedOperatorSubtaskState

```
public static class Builder {

   /** Ground truth of state, provided by job manager. */
   @Nonnull
   private final OperatorSubtaskState jobManagerState;

   /** (Local) alternatives to the job manager state. */
   @Nonnull
   private final List<OperatorSubtaskState> alternativesByPriority;

   /** Flag if the states have been restored. */
   private final boolean restored;

   public Builder(
      @Nonnull OperatorSubtaskState jobManagerState,
      @Nonnull List<OperatorSubtaskState> alternativesByPriority) {
      this(jobManagerState, alternativesByPriority, true);
   }

   public Builder(
      @Nonnull OperatorSubtaskState jobManagerState,
      @Nonnull List<OperatorSubtaskState> alternativesByPriority,
      boolean restored) {

      this.jobManagerState = jobManagerState;
      this.alternativesByPriority = alternativesByPriority;
      this.restored = restored;
   }

   public PrioritizedOperatorSubtaskState build() {
      int size = alternativesByPriority.size();
      List<StateObjectCollection<OperatorStateHandle>> managedOperatorAlternatives = new ArrayList<>(size);
      List<StateObjectCollection<KeyedStateHandle>> managedKeyedAlternatives = new ArrayList<>(size);
      List<StateObjectCollection<OperatorStateHandle>> rawOperatorAlternatives = new ArrayList<>(size);
      List<StateObjectCollection<KeyedStateHandle>> rawKeyedAlternatives = new ArrayList<>(size);

//按照优先级添加SubtaskState
      for (OperatorSubtaskState subtaskState : alternativesByPriority) {

         if (subtaskState != null) {
            managedKeyedAlternatives.add(subtaskState.getManagedKeyedState());
            rawKeyedAlternatives.add(subtaskState.getRawKeyedState());
            managedOperatorAlternatives.add(subtaskState.getManagedOperatorState());
            rawOperatorAlternatives.add(subtaskState.getRawOperatorState());
         }
      }

      // Key-groups should match.
      BiFunction<KeyedStateHandle, KeyedStateHandle, Boolean> keyedStateApprover =
         (ref, alt) -> ref.getKeyGroupRange().equals(alt.getKeyGroupRange());

      // State meta data should match.
      BiFunction<OperatorStateHandle, OperatorStateHandle, Boolean> operatorStateApprover =
         (ref, alt) -> ref.getStateNameToPartitionOffsets().equals(alt.getStateNameToPartitionOffsets());

      return new PrioritizedOperatorSubtaskState(
         resolvePrioritizedAlternatives(
            jobManagerState.getManagedKeyedState(),
            managedKeyedAlternatives,
            keyedStateApprover),
         resolvePrioritizedAlternatives(
            jobManagerState.getRawKeyedState(),
            rawKeyedAlternatives,
            keyedStateApprover),
         resolvePrioritizedAlternatives(
            jobManagerState.getManagedOperatorState(),
            managedOperatorAlternatives,
            operatorStateApprover),
         resolvePrioritizedAlternatives(
            jobManagerState.getRawOperatorState(),
            rawOperatorAlternatives,
            operatorStateApprover),
         restored);
   }
   
   
 protected <T extends StateObject> List<StateObjectCollection<T>> resolvePrioritizedAlternatives(
			StateObjectCollection<T> jobManagerState,
			List<StateObjectCollection<T>> alternativesByPriority,
			BiFunction<T, T, Boolean> approveFun) {

			// Nothing to resolve if there are no alternatives, or the ground truth has already no state, or if we can
			// assume that a rescaling happened because we find more than one handle in the JM state (this is more a sanity
			// check).
			if (alternativesByPriority == null
				|| alternativesByPriority.isEmpty()
				|| !jobManagerState.hasState()
				|| jobManagerState.size() != 1) {

				return Collections.singletonList(jobManagerState);
			}

			// As we know size is == 1
			T reference = jobManagerState.iterator().next();

			// This will contain the end result, we initialize it with the potential max. size.
			List<StateObjectCollection<T>> approved =
				new ArrayList<>(1 + alternativesByPriority.size());

			for (StateObjectCollection<T> alternative : alternativesByPriority) {

				// We found an alternative to the JM state if it has state, we have a 1:1 relationship, and the
				// approve-function signaled true.
				if (alternative != null
					&& alternative.hasState()
					&& alternative.size() == 1
					&& BooleanUtils.isTrue(approveFun.apply(reference, alternative.iterator().next()))) {

					approved.add(alternative);
				}
			}

			// Of course we include the ground truth as last alternative.
			// 从 JobManager 获取的状态作为最低优先级的备选
			approved.add(jobManagerState);
			return Collections.unmodifiableList(approved);
		}
	}
```