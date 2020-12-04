回顾之前的流程，TaskExecutor启动成功之后会调用回调函数onStart，建立一系列连接。其中JobLeaderListenerImp 监听JobManager的启动和leader选举，完成之后回调JobLeaderService的onRegistrationSuccess回调函数。onRegistrationSuccess调用JobLeaderListener的jobManagerGainedLeadership回调函数。

```
@Override
public void onStart() throws Exception {
   try {
      startTaskExecutorServices();
   } catch (Exception e) {
      final TaskManagerException exception = new TaskManagerException(String.format("Could not start the TaskExecutor %s", getAddress()), e);
      onFatalError(exception);
      throw exception;
   }

   startRegistrationTimeout();
}




private void startTaskExecutorServices() throws Exception {
		try {
			// start by connecting to the ResourceManager
			resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());

			// tell the task slot table who's responsible for the task slot actions
			taskSlotTable.start(new SlotActionsImpl(), getMainThreadExecutor());
	
			// start the job leader service
			jobLeaderService.start(getAddress(), getRpcService(), haServices, new JobLeaderListenerImpl());
	
			fileCache = new FileCache(taskManagerConfiguration.getTmpDirectories(), blobCacheService.getPermanentBlobService());
 	} catch (Exception e) {
			handleStartTaskExecutorServicesException(e);
		}
	}
```

除了TaskExecutor启动的时候建立连接，在有新的job申请slot资源的时候也会建立连接。回顾slot资源管理的过程，在TaskExecutor的requestSlot中会注册新的job，通过JobLeaderService的addJob，由highAvailabilityServices选取该job的JobManager的Leader节点，并启动JobManagerLeaderListener主节点的监听JobManagerLeaderListener。选取成功会回调JobManagerLeaderListener的notifyLeaderAddress函数。

```
public void notifyLeaderAddress(final @Nullable String leaderAddress, final @Nullable UUID leaderId) {
   if (stopped) {
      LOG.debug("{}'s leader retrieval listener reported a new leader for job {}. " +
         "However, the service is no longer running.", JobLeaderService.class.getSimpleName(), jobId);
   } else {
      final JobMasterId jobMasterId = JobMasterId.fromUuidOrNull(leaderId);

      LOG.debug("New leader information for job {}. Address: {}, leader id: {}.",
         jobId, leaderAddress, jobMasterId);

      if (leaderAddress == null || leaderAddress.isEmpty()) {
         // the leader lost leadership but there is no other leader yet.
         if (rpcConnection != null) {
            rpcConnection.close();
         }

         jobLeaderListener.jobManagerLostLeadership(jobId, currentJobMasterId);

         currentJobMasterId = jobMasterId;
      } else {
         currentJobMasterId = jobMasterId;

         if (rpcConnection != null) {
            // check if we are already trying to connect to this leader
            if (!Objects.equals(jobMasterId, rpcConnection.getTargetLeaderId())) {
               rpcConnection.close();

               rpcConnection = new JobManagerRegisteredRpcConnection(
                  LOG,
                  leaderAddress,
                  jobMasterId,
                  rpcService.getExecutor());
            }
         } else {
            rpcConnection = new JobManagerRegisteredRpcConnection(
               LOG,
               leaderAddress,
               jobMasterId,
               rpcService.getExecutor());
         }

         // double check for a concurrent stop operation
         if (stopped) {
            rpcConnection.close();
         } else {
            LOG.info("Try to register at job manager {} with leader id {}.", leaderAddress, leaderId);
            rpcConnection.start();
         }
      }
   }
}
```

notifyLeaderAddress会初始化JobManagerRegisteredRpcConnection，即JobManager主节点与TaskExecutor的连接类。连接成功会调用onRegistrationSuccess回调方法，同样会调用jobManagerGainedLeadership回调函数

```
private final class JobManagerRegisteredRpcConnection extends RegisteredRpcConnection<JobMasterId, JobMasterGateway, JMTMRegistrationSuccess> {

      JobManagerRegisteredRpcConnection(
            Logger log,
            String targetAddress,
            JobMasterId jobMasterId,
            Executor executor) {
         super(log, targetAddress, jobMasterId, executor);
      }

      @Override
      protected RetryingRegistration<JobMasterId, JobMasterGateway, JMTMRegistrationSuccess> generateRegistration() {
         return new JobLeaderService.JobManagerRetryingRegistration(
               LOG,
               rpcService,
               "JobManager",
               JobMasterGateway.class,
               getTargetAddress(),
               getTargetLeaderId(),
               retryingRegistrationConfiguration,
               ownerAddress,
               ownLocation);
      }

      @Override
      protected void onRegistrationSuccess(JMTMRegistrationSuccess success) {
         // filter out old registration attempts
         if (Objects.equals(getTargetLeaderId(), currentJobMasterId)) {
            log.info("Successful registration at job manager {} for job {}.", getTargetAddress(), jobId);

            jobLeaderListener.jobManagerGainedLeadership(jobId, getTargetGateway(), success);
         } else {
            log.debug("Encountered obsolete JobManager registration success from {} with leader session ID {}.", getTargetAddress(), getTargetLeaderId());
         }
      }

      @Override
      protected void onRegistrationFailure(Throwable failure) {
         // filter out old registration attempts
         if (Objects.equals(getTargetLeaderId(), currentJobMasterId)) {
            log.info("Failed to register at job  manager {} for job {}.", getTargetAddress(), jobId);
            jobLeaderListener.handleError(failure);
         } else {
            log.debug("Obsolete JobManager registration failure from {} with leader session ID {}.", getTargetAddress(), getTargetLeaderId(), failure);
         }
      }
   }
}
```

JobLeaderListenerImpl类及jobManagerLostLeadership回调函数

```
private final class JobLeaderListenerImpl implements JobLeaderListener {

	@Override
	public void jobManagerGainedLeadership(
		final JobID jobId,
		final JobMasterGateway jobManagerGateway,
		final JMTMRegistrationSuccess registrationMessage) {
		runAsync(
			() ->
			//与JobManager的leader节点建立连接
				establishJobManagerConnection(
					jobId,
					jobManagerGateway,
					registrationMessage));
	}
	
	@Override
	public void jobManagerLostLeadership(final JobID jobId, final JobMasterId jobMasterId) {
		log.info("JobManager for job {} with leader id {} lost leadership.", jobId, jobMasterId);
	
		runAsync(() ->
			closeJobManagerConnection(
				jobId,
				new Exception("Job leader for job id " + jobId + " lost leadership.")));
	}
	
	@Override
	public void handleError(Throwable throwable) {
		onFatalError(throwable);
	}

}
```

jobManagerGainedLeadership函数再调用TaskExecutor的establishJobManagerConnection函数。

```
private void establishJobManagerConnection(JobID jobId, final JobMasterGateway jobMasterGateway, JMTMRegistrationSuccess registrationSuccess) {

   if (jobManagerTable.contains(jobId)) {
      JobManagerConnection oldJobManagerConnection = jobManagerTable.get(jobId);

      if (Objects.equals(oldJobManagerConnection.getJobMasterId(), jobMasterGateway.getFencingToken())) {
         // we already are connected to the given job manager
         log.debug("Ignore JobManager gained leadership message for {} because we are already connected to it.", jobMasterGateway.getFencingToken());
         return;
      } else {
         closeJobManagerConnection(jobId, new Exception("Found new job leader for job id " + jobId + '.'));
      }
   }

   log.info("Establish JobManager connection for job {}.", jobId);

   ResourceID jobManagerResourceID = registrationSuccess.getResourceID();
   
   //初始化JobManagerConnection，用于存储TaskExecutor所需的JobManager的信息
   JobManagerConnection newJobManagerConnection = associateWithJobManager(
         jobId,
         jobManagerResourceID,
         jobMasterGateway);
         
         //将JobManagerConnection存入map
   jobManagerConnections.put(jobManagerResourceID, newJobManagerConnection);
   jobManagerTable.put(jobId, newJobManagerConnection);

   // monitor the job manager as heartbeat target
   jobManagerHeartbeatManager.monitorTarget(jobManagerResourceID, new HeartbeatTarget<AccumulatorReport>() {
      @Override
      public void receiveHeartbeat(ResourceID resourceID, AccumulatorReport payload) {
         jobMasterGateway.heartbeatFromTaskManager(resourceID, payload);
      }

      @Override
      public void requestHeartbeat(ResourceID resourceID, AccumulatorReport payload) {
         // request heartbeat will never be called on the task manager side
      }
   });

//向JobManager提供需要的slot资源，在TaskManager上报Slot资源的过程已经分析。
   offerSlotsToJobManager(jobId);
}
```

associateWithJobManager方法

```
private JobManagerConnection associateWithJobManager(
      JobID jobID,
      ResourceID resourceID,
      JobMasterGateway jobMasterGateway) {
   checkNotNull(jobID);
   checkNotNull(resourceID);
   checkNotNull(jobMasterGateway);

   TaskManagerActions taskManagerActions = new TaskManagerActionsImpl(jobMasterGateway);

   CheckpointResponder checkpointResponder = new RpcCheckpointResponder(jobMasterGateway);
   GlobalAggregateManager aggregateManager = new RpcGlobalAggregateManager(jobMasterGateway);

   final LibraryCacheManager libraryCacheManager = new BlobLibraryCacheManager(
      blobCacheService.getPermanentBlobService(),
      taskManagerConfiguration.getClassLoaderResolveOrder(),
      taskManagerConfiguration.getAlwaysParentFirstLoaderPatterns());

//ResultPartition监听
   ResultPartitionConsumableNotifier resultPartitionConsumableNotifier = new RpcResultPartitionConsumableNotifier(
      jobMasterGateway,
      getRpcService().getExecutor(),
      taskManagerConfiguration.getTimeout());

   PartitionProducerStateChecker partitionStateChecker = new RpcPartitionStateChecker(jobMasterGateway);

//注册可查询的State
   registerQueryableState(jobID, jobMasterGateway);

   return new JobManagerConnection(
      jobID,
      resourceID,
      jobMasterGateway,
      taskManagerActions,
      checkpointResponder,
      aggregateManager,
      libraryCacheManager,
      resultPartitionConsumableNotifier,
      partitionStateChecker);
}
```

JobManagerConnection类

```
public class JobManagerConnection {

   // Job id related with the job manager
   private final JobID jobID;

   // The unique id used for identifying the job manager
   private final ResourceID resourceID;

   // Gateway to the job master
   private final JobMasterGateway jobMasterGateway;

   // Task manager actions with respect to the connected job manager
   private final TaskManagerActions taskManagerActions;

   // Checkpoint responder for the specific job manager
   private final CheckpointResponder checkpointResponder;

   // GlobalAggregateManager interface to job manager
   private final GlobalAggregateManager aggregateManager;

   // Library cache manager connected to the specific job manager
   private final LibraryCacheManager libraryCacheManager;

   // Result partition consumable notifier for the specific job manager
   private final ResultPartitionConsumableNotifier resultPartitionConsumableNotifier;

   // Partition state checker for the specific job manager
   private final PartitionProducerStateChecker partitionStateChecker;
```