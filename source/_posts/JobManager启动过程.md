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

以下过程大概是提交job之后，创建JobManager，在JobManager中创建JobMaster，在JobMaster中创建Scheduler等

```
private CompletableFuture<Void> persistAndRunJob(JobGraph jobGraph) throws Exception {
//保存jobGraph
		jobGraphWriter.putJobGraph(jobGraph);

		final CompletableFuture<Void> runJobFuture = runJob(jobGraph);

		return runJobFuture.whenComplete(BiConsumerWithException.unchecked((Object ignored, Throwable throwable) -> {
			if (throwable != null) {
				jobGraphWriter.removeJobGraph(jobGraph.getJobID());
			}
		}));
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

			// libraries and class loader first
			try {
				libraryCacheManager.registerJob(
						jobGraph.getJobID(), jobGraph.getUserJarBlobKeys(), jobGraph.getClasspaths());
			} catch (IOException e) {
				throw new Exception("Cannot set up the user code libraries: " + e.getMessage(), e);
			}

			final ClassLoader userCodeLoader = libraryCacheManager.getClassLoader(jobGraph.getJobID());
			if (userCodeLoader == null) {
				throw new Exception("The user code class loader could not be initialized.");
			}

			// high availability services next
			this.runningJobsRegistry = haServices.getRunningJobsRegistry();
			//获取HA选举服务
			this.leaderElectionService = haServices.getJobManagerLeaderElectionService(jobGraph.getJobID());

			this.leaderGatewayFuture = new CompletableFuture<>();

			// now start the JobManager，调用JobMaster的构造函数
			this.jobMasterService = jobMasterFactory.createJobMasterService(jobGraph, this, userCodeLoader);
		}
		......
	}
	
	
	
	
	
	public JobMaster createJobMasterService(
			JobGraph jobGraph,
			OnCompletionActions jobCompletionActions,
			ClassLoader userCodeClassloader) throws Exception {

		return new JobMaster(
			rpcService,
			jobMasterConfiguration,
			ResourceID.generate(),
			jobGraph,
			haServices,
			slotPoolFactory,
			schedulerFactory,
			jobManagerSharedServices,
			heartbeatServices,
			jobManagerJobMetricGroupFactory,
			jobCompletionActions,
			fatalErrorHandler,
			userCodeClassloader,
			schedulerNGFactory,
			shuffleMaster,
			lookup -> new JobMasterPartitionTrackerImpl(
				jobGraph.getJobID(),
				shuffleMaster,
				lookup
			));
	}
	
	
	
	
	//进入JobMaster类构造函数
	public JobMaster(
			......
			JobGraph jobGraph,
			......) throws Exception {

		......
		resourceManagerLeaderRetriever = highAvailabilityServices.getResourceManagerLeaderRetriever();

//创建SlotPool，用来给job申请和分配slot资源
		this.slotPool = checkNotNull(slotPoolFactory).createSlotPool(jobGraph.getJobID());

//创建Scheduler，SchedulerImpl类用来使用slotSelectionStrategy把task分配给slot
		this.scheduler = checkNotNull(schedulerFactory).createScheduler(slotPool);

//用来保存已连接的TaskManager
		this.registeredTaskManagers = new HashMap<>(4);
		this.partitionTracker = checkNotNull(partitionTrackerFactory)
			.create(resourceID -> {
				Tuple2<TaskManagerLocation, TaskExecutorGateway> taskManagerInfo = registeredTaskManagers.get(resourceID);
				if (taskManagerInfo == null) {
					return Optional.empty();
				}

				return Optional.of(taskManagerInfo.f1);
			});

		this.backPressureStatsTracker = checkNotNull(jobManagerSharedServices.getBackPressureStatsTracker());

		this.shuffleMaster = checkNotNull(shuffleMaster);

		this.jobManagerJobMetricGroup = jobMetricGroupFactory.create(jobGraph);
		//创建flink job的调度类，通过DefaultSchedulerFactory工厂类的createInstance创建实例
		this.schedulerNG = createScheduler(jobManagerJobMetricGroup);
		this.jobStatusListener = null;

		this.resourceManagerConnection = null;
		this.establishedResourceManagerConnection = null;

		this.accumulators = new HashMap<>();
		this.taskManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();
		this.resourceManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();
	}
```

schedulerNG用于调度提交的job，管理调度策略，slot分配策略，生成ExecutionGraph，根据savapoint及checkpoint启动等。

创建好JobManagerRunner之后，回到Dispatcher的runJob方法，调用startJobManagerRunner启动JobManager。

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

JobManagerRunnerImpl类

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

`JobMaster` 启动后会和 `ResourceManager` 建立连接，连接被封装为 `ResourceManagerConnection`。一旦连接建立之后，`JobMaster` 就可以通过 RPC 调用和 `ResourceManager` 进行通信了

```
private void startJobMasterServices() throws Exception {
//启动心跳服务
   startHeartbeatServices();

   // start the slot pool make sure the slot pool now accepts messages for this leader
   //启动slotPool及scheduler
   slotPool.start(getFencingToken(), getAddress(), getMainThreadExecutor());
   scheduler.start(getMainThreadExecutor());

   //TODO: Remove once the ZooKeeperLeaderRetrieval returns the stored address upon start
   // try to reconnect to previously known leader
   reconnectToResourceManager(new FlinkException("Starting JobMaster component."));

   // job is ready to go, try to establish connection with resource manager
   //   - activate leader retrieval for the resource manager
   //   - on notification of the leader, the connection will be established and
   //     the slot pool will start requesting slots
   //如果previously known leader连接失败，则等待ResourceManagerLeader选举，然后调用reconnectToResourceManager
   resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());
}
```

最终调用connectToResourceManager将连接封装为ResourceManagerConnection

```
private void connectToResourceManager() {
   assert(resourceManagerAddress != null);
   assert(resourceManagerConnection == null);
   assert(establishedResourceManagerConnection == null);

   log.info("Connecting to ResourceManager {}", resourceManagerAddress);

   resourceManagerConnection = new ResourceManagerConnection(
      log,
      jobGraph.getJobID(),
      resourceId,
      getAddress(),
      getFencingToken(),
      resourceManagerAddress.getAddress(),
      resourceManagerAddress.getResourceManagerId(),
      scheduledExecutorService);

   resourceManagerConnection.start();
}
```

resourceManagerConnection启动

```
public void start() {
   checkState(!closed, "The RPC connection is already closed");
   checkState(!isConnected() && pendingRegistration == null, "The RPC connection is already started");

   final RetryingRegistration<F, G, S> newRegistration = createNewRegistration();

   if (REGISTRATION_UPDATER.compareAndSet(this, null, newRegistration)) {
      newRegistration.startRegistration();
   } else {
      // concurrent start operation
      newRegistration.cancel();
   }
}
```

createNewRegistration创建与ResourceManager的连接

```
private RetryingRegistration<F, G, S> createNewRegistration() {
   RetryingRegistration<F, G, S> newRegistration = checkNotNull(generateRegistration());

   CompletableFuture<Tuple2<G, S>> future = newRegistration.getFuture();

   future.whenCompleteAsync(
      (Tuple2<G, S> result, Throwable failure) -> {
         if (failure != null) {
            if (failure instanceof CancellationException) {
               // we ignore cancellation exceptions because they originate from cancelling
               // the RetryingRegistration
               log.debug("Retrying registration towards {} was cancelled.", targetAddress);
            } else {
               // this future should only ever fail if there is a bug, not if the registration is declined
               onRegistrationFailure(failure);
            }
         } else {
            targetGateway = result.f0;
            onRegistrationSuccess(result.f1);
         }
      }, executor);

   return newRegistration;
}
```

成功后回调onRegistrationSuccess建立连接

```
protected void onRegistrationSuccess(final JobMasterRegistrationSuccess success) {
   runAsync(() -> {
      // filter out outdated connections
      //noinspection ObjectEquality
      if (this == resourceManagerConnection) {
         establishResourceManagerConnection(success);
      }
   });
}
```

建立的连接封装为EstablishedResourceManagerConnection类

```
private void establishResourceManagerConnection(final JobMasterRegistrationSuccess success) {
   final ResourceManagerId resourceManagerId = success.getResourceManagerId();

   // verify the response with current connection
   if (resourceManagerConnection != null
         && Objects.equals(resourceManagerConnection.getTargetLeaderId(), resourceManagerId)) {

      log.info("JobManager successfully registered at ResourceManager, leader id: {}.", resourceManagerId);

      final ResourceManagerGateway resourceManagerGateway = resourceManagerConnection.getTargetGateway();

      final ResourceID resourceManagerResourceId = success.getResourceManagerResourceId();

      establishedResourceManagerConnection = new EstablishedResourceManagerConnection(
         resourceManagerGateway,
         resourceManagerResourceId);

      slotPool.connectToResourceManager(resourceManagerGateway);

      resourceManagerHeartbeatManager.monitorTarget(resourceManagerResourceId, new HeartbeatTarget<Void>() {
         @Override
         public void receiveHeartbeat(ResourceID resourceID, Void payload) {
            resourceManagerGateway.heartbeatFromJobManager(resourceID);
         }

         @Override
         public void requestHeartbeat(ResourceID resourceID, Void payload) {
            // request heartbeat will never be called on the job manager side
         }
      });
   } else {
      log.debug("Ignoring resource manager connection to {} because it's duplicated or outdated.", resourceManagerId);

   }
}
```

创建好newRegistration之后启动，newRegistration.startRegistration()向ResourceManager注册JobMaster

```
public void startRegistration() {
   if (canceled) {
      // we already got canceled
      return;
   }

   try {
      // trigger resolution of the target address to a callable gateway
      final CompletableFuture<G> rpcGatewayFuture;

//获取ResourceManagerGateway地址
      if (FencedRpcGateway.class.isAssignableFrom(targetType)) {
         rpcGatewayFuture = (CompletableFuture<G>) rpcService.connect(
            targetAddress,
            fencingToken,
            targetType.asSubclass(FencedRpcGateway.class));
      } else {
         rpcGatewayFuture = rpcService.connect(targetAddress, targetType);
      }

      // upon success, start the registration attempts
      //获取ResourceManagerGateway成功之后向ResourceManager注册
      CompletableFuture<Void> rpcGatewayAcceptFuture = rpcGatewayFuture.thenAcceptAsync(
         (G rpcGateway) -> {
            log.info("Resolved {} address, beginning registration", targetName);
            register(rpcGateway, 1, retryingRegistrationConfiguration.getInitialRegistrationTimeoutMillis());
         },
         rpcService.getExecutor());

      ......
}
```

```
private void register(final G gateway, final int attempt, final long timeoutMillis) {
   // eager check for canceling to avoid some unnecessary work
   if (canceled) {
      return;
   }

   try {
      log.info("Registration at {} attempt {} (timeout={}ms)", targetName, attempt, timeoutMillis);
      //开始注册
      CompletableFuture<RegistrationResponse> registrationFuture = invokeRegistration(gateway, fencingToken, timeoutMillis);

      // if the registration was successful, let the TaskExecutor know
      CompletableFuture<Void> registrationAcceptFuture = registrationFuture.thenAcceptAsync(
         (RegistrationResponse result) -> {
            if (!isCanceled()) {
               if (result instanceof RegistrationResponse.Success) {
                  // registration successful!
                  S success = (S) result;
                  completionFuture.complete(Tuple2.of(gateway, success));
               }
               else {
                  // registration refused or unknown
                  if (result instanceof RegistrationResponse.Decline) {
                     RegistrationResponse.Decline decline = (RegistrationResponse.Decline) result;
                     log.info("Registration at {} was declined: {}", targetName, decline.getReason());
                  } else {
                     log.error("Received unknown response to registration attempt: {}", result);
                  }

                  log.info("Pausing and re-attempting registration in {} ms", retryingRegistrationConfiguration.getRefusedDelayMillis());
                  registerLater(gateway, 1, retryingRegistrationConfiguration.getInitialRegistrationTimeoutMillis(), retryingRegistrationConfiguration.getRefusedDelayMillis());
               }
            }
         },
         rpcService.getExecutor());

......
}
```

JobMaster的invokeRegistration，调用ResourceManagerGateway的注册方法

```
protected CompletableFuture<RegistrationResponse> invokeRegistration(
            ResourceManagerGateway gateway, ResourceManagerId fencingToken, long timeoutMillis) {
         Time timeout = Time.milliseconds(timeoutMillis);

         return gateway.registerJobManager(
            jobMasterId,
            jobManagerResourceID,
            jobManagerRpcAddress,
            jobID,
            timeout);
      }
   };
}
```

ResourceManager注册JobManager的方法

```
public CompletableFuture<RegistrationResponse> registerJobManager(
      final JobMasterId jobMasterId,
      final ResourceID jobManagerResourceId,
      final String jobManagerAddress,
      final JobID jobId,
      final Time timeout) {

   checkNotNull(jobMasterId);
   checkNotNull(jobManagerResourceId);
   checkNotNull(jobManagerAddress);
   checkNotNull(jobId);

   if (!jobLeaderIdService.containsJob(jobId)) {
      try {
         jobLeaderIdService.addJob(jobId);
      } catch (Exception e) {
         ResourceManagerException exception = new ResourceManagerException("Could not add the job " +
            jobId + " to the job id leader service.", e);

            onFatalError(exception);

         log.error("Could not add job {} to job leader id service.", jobId, e);
         return FutureUtils.completedExceptionally(exception);
      }
   }

   log.info("Registering job manager {}@{} for job {}.", jobMasterId, jobManagerAddress, jobId);

   CompletableFuture<JobMasterId> jobMasterIdFuture;

   try {
      jobMasterIdFuture = jobLeaderIdService.getLeaderId(jobId);
   } catch (Exception e) {
      // we cannot check the job leader id so let's fail
      // TODO: Maybe it's also ok to skip this check in case that we cannot check the leader id
      ResourceManagerException exception = new ResourceManagerException("Cannot obtain the " +
         "job leader id future to verify the correct job leader.", e);

         onFatalError(exception);

      log.debug("Could not obtain the job leader id future to verify the correct job leader.");
      return FutureUtils.completedExceptionally(exception);
   }

   CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getRpcService().connect(jobManagerAddress, jobMasterId, JobMasterGateway.class);

   CompletableFuture<RegistrationResponse> registrationResponseFuture = jobMasterGatewayFuture.thenCombineAsync(
      jobMasterIdFuture,
      (JobMasterGateway jobMasterGateway, JobMasterId leadingJobMasterId) -> {
         if (Objects.equals(leadingJobMasterId, jobMasterId)) {
            return registerJobMasterInternal(
               jobMasterGateway,
               jobId,
               jobManagerAddress,
               jobManagerResourceId);
         } else {
            final String declineMessage = String.format(
               "The leading JobMaster id %s did not match the received JobMaster id %s. " +
               "This indicates that a JobMaster leader change has happened.",
               leadingJobMasterId,
               jobMasterId);
            log.debug(declineMessage);
            return new RegistrationResponse.Decline(declineMessage);
         }
      },
      getMainThreadExecutor());

   // handle exceptions which might have occurred in one of the futures inputs of combine
   return registrationResponseFuture.handleAsync(
      (RegistrationResponse registrationResponse, Throwable throwable) -> {
         if (throwable != null) {
            if (log.isDebugEnabled()) {
               log.debug("Registration of job manager {}@{} failed.", jobMasterId, jobManagerAddress, throwable);
            } else {
               log.info("Registration of job manager {}@{} failed.", jobMasterId, jobManagerAddress);
            }

            return new RegistrationResponse.Decline(throwable.getMessage());
         } else {
            return registrationResponse;
         }
      },
      getRpcService().getExecutor());
}
```

```
private RegistrationResponse registerJobMasterInternal(
   final JobMasterGateway jobMasterGateway,
   JobID jobId,
   String jobManagerAddress,
   ResourceID jobManagerResourceId) {
   if (jobManagerRegistrations.containsKey(jobId)) {
      JobManagerRegistration oldJobManagerRegistration = jobManagerRegistrations.get(jobId);

      if (Objects.equals(oldJobManagerRegistration.getJobMasterId(), jobMasterGateway.getFencingToken())) {
         // same registration
         log.debug("Job manager {}@{} was already registered.", jobMasterGateway.getFencingToken(), jobManagerAddress);
      } else {
         // tell old job manager that he is no longer the job leader
         disconnectJobManager(
            oldJobManagerRegistration.getJobID(),
            new Exception("New job leader for job " + jobId + " found."));

         JobManagerRegistration jobManagerRegistration = new JobManagerRegistration(
            jobId,
            jobManagerResourceId,
            jobMasterGateway);
         jobManagerRegistrations.put(jobId, jobManagerRegistration);
         jmResourceIdRegistrations.put(jobManagerResourceId, jobManagerRegistration);
      }
   } else {
      // new registration for the job
      JobManagerRegistration jobManagerRegistration = new JobManagerRegistration(
         jobId,
         jobManagerResourceId,
         jobMasterGateway);
      jobManagerRegistrations.put(jobId, jobManagerRegistration);
      jmResourceIdRegistrations.put(jobManagerResourceId, jobManagerRegistration);
   }

   log.info("Registered job manager {}@{} for job {}.", jobMasterGateway.getFencingToken(), jobManagerAddress, jobId);

   jobManagerHeartbeatManager.monitorTarget(jobManagerResourceId, new HeartbeatTarget<Void>() {
      @Override
      public void receiveHeartbeat(ResourceID resourceID, Void payload) {
         // the ResourceManager will always send heartbeat requests to the JobManager
      }

      @Override
      public void requestHeartbeat(ResourceID resourceID, Void payload) {
         jobMasterGateway.heartbeatFromResourceManager(resourceID);
      }
   });

   return new JobMasterRegistrationSuccess(
      getFencingToken(),
      resourceId);
}
```

生成JobManagerRegistration，之后就可以和ResourceManager通信了