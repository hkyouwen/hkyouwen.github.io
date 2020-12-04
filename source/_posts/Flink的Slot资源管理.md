![](1.png)

上图是TaskManager 上报slot资源的大致流程。

TaskManager 管理slot的逻辑在TaskExecutor类里面。Resoucemanager组件对所有TaskManager 的slot资源进行管理，因此TaskExecutor与Resoucemanager建立连接之后会报告slot的分配情况。

```
private void establishResourceManagerConnection(
			ResourceManagerGateway resourceManagerGateway,
			ResourceID resourceManagerResourceId,
			InstanceID taskExecutorRegistrationId,
			ClusterInformation clusterInformation) {

//发送slot报告
		final CompletableFuture<Acknowledge> slotReportResponseFuture = resourceManagerGateway.sendSlotReport(
			getResourceID(),
			taskExecutorRegistrationId,
			taskSlotTable.createSlotReport(getResourceID()),
			taskManagerConfiguration.getTimeout());

		slotReportResponseFuture.whenCompleteAsync(
			(acknowledge, throwable) -> {
				if (throwable != null) {
					reconnectToResourceManager(new TaskManagerException("Failed to send initial slot report to ResourceManager.", throwable));
				}
			}, getMainThreadExecutor());

		......
	}
```

TaskSlotTableImpl类的createSlotReport，收集该TaskManager 的slot资源情况，TaskSlotTableImpl类是TaskSlotTable实现类。

```
public SlotReport createSlotReport(ResourceID resourceId) {
   List<SlotStatus> slotStatuses = new ArrayList<>();

   for (int i = 0; i < numberSlots; i++) {
      SlotID slotId = new SlotID(resourceId, i);
      SlotStatus slotStatus;
      \\所有的taskSlots
      if (taskSlots.containsKey(i)) {
         TaskSlot<T> taskSlot = taskSlots.get(i);

         slotStatus = new SlotStatus(
            slotId,
            taskSlot.getResourceProfile(),
            taskSlot.getJobId(),
            taskSlot.getAllocationId());
      } else {
      \\初始slot状态未分配id
         slotStatus = new SlotStatus(
            slotId,
            defaultSlotResourceProfile,
            null,
            null);
      }

      slotStatuses.add(slotStatus);
   }

\\allocation slot id
   for (TaskSlot<T> taskSlot : allocatedSlots.values()) {
      if (taskSlot.getIndex() < 0) {
         SlotID slotID = SlotID.generateDynamicSlotID(resourceId);
         SlotStatus slotStatus = new SlotStatus(
            slotID,
            taskSlot.getResourceProfile(),
            taskSlot.getJobId(),
            taskSlot.getAllocationId());
         slotStatuses.add(slotStatus);
      }
   }

   final SlotReport slotReport = new SlotReport(slotStatuses);

   return slotReport;
}
```

先看一下TaskSlotTable的初始化，这个过程是在启动TaskManager的过程中。TaskManagerRunner类的startTaskManager方法启动TaskExecutor，同时初始化TaskManagerServices，初始化TaskManagerServices之前创建TaskSlotTable，做为TaskManagerServices的一个成员变量。createTaskSlotTable方法初始化TaskSlotTableImpl。

```
TaskManagerServices taskManagerServices = TaskManagerServices.fromConfiguration(
   taskManagerServicesConfiguration,
   taskManagerMetricGroup.f1,
   rpcService.getExecutor());
```

```
public static TaskManagerServices fromConfiguration(
      TaskManagerServicesConfiguration taskManagerServicesConfiguration,
      MetricGroup taskManagerMetricGroup,
      Executor taskIOExecutor) throws Exception {

  ......

   final TaskSlotTable<Task> taskSlotTable = createTaskSlotTable(
      taskManagerServicesConfiguration.getNumberOfSlots(),
      taskManagerServicesConfiguration.getTaskExecutorResourceSpec(),
      taskManagerServicesConfiguration.getTimerServiceShutdownTimeout(),
      taskManagerServicesConfiguration.getPageSize());

  ......
  return new TaskManagerServices(..
}
```

```
private static TaskSlotTable<Task> createTaskSlotTable(
      final int numberOfSlots,
      final TaskExecutorResourceSpec taskExecutorResourceSpec,
      final long timerServiceShutdownTimeout,
      final int pageSize) {
   final TimerService<AllocationID> timerService = new TimerService<>(
      new ScheduledThreadPoolExecutor(1),
      timerServiceShutdownTimeout);
   return new TaskSlotTableImpl<>(
      numberOfSlots,
      TaskExecutorResourceUtils.generateTotalAvailableResourceProfile(taskExecutorResourceSpec),
      TaskExecutorResourceUtils.generateDefaultSlotResourceProfile(taskExecutorResourceSpec, numberOfSlots),
      pageSize,
      timerService);
}
```

TaskSlotTable生成slot的报告之后向resourceManagerGateway发送slot资源信息，

ResourceManager类：

```
public CompletableFuture<Acknowledge> sendSlotReport(ResourceID taskManagerResourceId, InstanceID taskManagerRegistrationId, SlotReport slotReport, Time timeout) {
   final WorkerRegistration<WorkerType> workerTypeWorkerRegistration = taskExecutors.get(taskManagerResourceId);

   if (workerTypeWorkerRegistration.getInstanceID().equals(taskManagerRegistrationId)) {
      slotManager.registerTaskManager(workerTypeWorkerRegistration, slotReport);
      return CompletableFuture.completedFuture(Acknowledge.get());
   } else {
      return FutureUtils.completedExceptionally(new ResourceManagerException(String.format("Unknown TaskManager registration id %s.", taskManagerRegistrationId)));
   }
}
```

进入SlotManagerImpl类,向SlotManager注册TaskManager

```
public void registerTaskManager(final TaskExecutorConnection taskExecutorConnection, SlotReport initialSlotReport) {

   // we identify task managers by their instance id
   //已经注册的taskManager上报更新SlotStatus
   if (taskManagerRegistrations.containsKey(taskExecutorConnection.getInstanceID())) {
      reportSlotStatus(taskExecutorConnection.getInstanceID(), initialSlotReport);
   } else {
   //第一次注册的TaskManager，新注册TaskManager和slot
      // first register the TaskManager
      ArrayList<SlotID> reportedSlots = new ArrayList<>();

      for (SlotStatus slotStatus : initialSlotReport) {
         reportedSlots.add(slotStatus.getSlotID());
      }

      TaskManagerRegistration taskManagerRegistration = new TaskManagerRegistration(
         taskExecutorConnection,
         reportedSlots);

//添加新的taskManager的id
      taskManagerRegistrations.put(taskExecutorConnection.getInstanceID(), taskManagerRegistration);

      // next register the new slots
      //register新的slot
      for (SlotStatus slotStatus : initialSlotReport) {
         registerSlot(
            slotStatus.getSlotID(),
            slotStatus.getAllocationID(),
            slotStatus.getJobID(),
            slotStatus.getResourceProfile(),
            taskExecutorConnection);
      }
   }

}
```

SlotManagerImpl注册新的slot

```
private void registerSlot(
      SlotID slotId,
      AllocationID allocationId,
      JobID jobId,
      ResourceProfile resourceProfile,
      TaskExecutorConnection taskManagerConnection) {

//删除老的同名slot
   if (slots.containsKey(slotId)) {
      // remove the old slot first
      removeSlot(
         slotId,
         new SlotManagerException(
            String.format(
               "Re-registration of slot %s. This indicates that the TaskExecutor has re-connected.",
               slotId)));
   }

//初始化TaskManagerSlot类并将<SlotId,TaskManagerSlot>键值对存入slots
   final TaskManagerSlot slot = createAndRegisterTaskManagerSlot(slotId, resourceProfile, taskManagerConnection);

   final PendingTaskManagerSlot pendingTaskManagerSlot;

   if (allocationId == null) {
   //这个 slot 还没有被分配，则找到和当前 slot 的计算资源相匹配的 PendingTaskManagerSlot
      pendingTaskManagerSlot = findExactlyMatchingPendingTaskManagerSlot(resourceProfile);
   } else {
   //这个 slot 已经被分配了
      pendingTaskManagerSlot = null;
   }

   if (pendingTaskManagerSlot == null) {
   //两种可能： 1）slot已经被分配了 2）没有匹配的 PendingTaskManagerSlot
      updateSlot(slotId, allocationId, jobId);
   } else {
   // 新注册的 slot 能够满足 PendingTaskManagerSlot 的要求
      pendingSlots.remove(pendingTaskManagerSlot.getTaskManagerSlotId());
      final PendingSlotRequest assignedPendingSlotRequest = pendingTaskManagerSlot.getAssignedPendingSlotRequest();
     // PendingTaskManagerSlot 可能有关联的 PedningSlotRequest
      if (assignedPendingSlotRequest == null) {
      //没有关联的 PedningSlotRequest，则将 slot 是 Free 状态
         handleFreeSlot(slot);
      } else {
      //有关联的 PedningSlotRequest，则这个 request 可以被满足，分配 slot
         assignedPendingSlotRequest.unassignPendingTaskManagerSlot();
         allocateSlot(slot, assignedPendingSlotRequest);
      }
   }
}
```

handleFreeSlot方法先查找是否有能够满足的 PendingSlotRequest，如果没有将该TaskManagerSlot放入freeSlots列表中

```
private void handleFreeSlot(TaskManagerSlot freeSlot) {
   Preconditions.checkState(freeSlot.getState() == TaskManagerSlot.State.FREE);

   PendingSlotRequest pendingSlotRequest = findMatchingRequest(freeSlot.getResourceProfile());

   if (null != pendingSlotRequest) {
      allocateSlot(freeSlot, pendingSlotRequest);
   } else {
      freeSlots.put(freeSlot.getSlotId(), freeSlot);
   }
}
```

SlotManagerImpl分配slot

```
private void allocateSlot(TaskManagerSlot taskManagerSlot, PendingSlotRequest pendingSlotRequest) {
   Preconditions.checkState(taskManagerSlot.getState() == TaskManagerSlot.State.FREE);

   TaskExecutorConnection taskExecutorConnection = taskManagerSlot.getTaskManagerConnection();
   TaskExecutorGateway gateway = taskExecutorConnection.getTaskExecutorGateway();

   final CompletableFuture<Acknowledge> completableFuture = new CompletableFuture<>();
   final AllocationID allocationId = pendingSlotRequest.getAllocationId();
   final SlotID slotId = taskManagerSlot.getSlotId();
   final InstanceID instanceID = taskManagerSlot.getInstanceId();

//taskManagerSlot 状态变为 PENDING
   taskManagerSlot.assignPendingSlotRequest(pendingSlotRequest);
   pendingSlotRequest.setRequestFuture(completableFuture);

//如果有 PendingTaskManager 指派给当前 pendingSlotRequest，要先解除关联
   returnPendingTaskManagerSlotIfAssigned(pendingSlotRequest);

   TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceID);

   if (taskManagerRegistration == null) {
      throw new IllegalStateException("Could not find a registered task manager for instance id " +
         instanceID + '.');
   }

   taskManagerRegistration.markUsed();

   // RPC call to the task manager
   // 通过 RPC 调用向 TaskExecutor 请求 slot
   CompletableFuture<Acknowledge> requestFuture = gateway.requestSlot(
      slotId,
      pendingSlotRequest.getJobId(),
      allocationId,
      pendingSlotRequest.getResourceProfile(),
      pendingSlotRequest.getTargetAddress(),
      resourceManagerId,
      taskManagerRequestTimeout);

//RPC调用的请求完成
   requestFuture.whenComplete(
      (Acknowledge acknowledge, Throwable throwable) -> {
         if (acknowledge != null) {
            completableFuture.complete(acknowledge);
         } else {
            completableFuture.completeExceptionally(throwable);
         }
      });

//PendingSlotRequest 请求完成的回调函数
		//PendingSlotRequest 请求完成可能是由于上面 RPC 调用完成，也可能是因为 PendingSlotRequest 被取消
   completableFuture.whenCompleteAsync(
      (Acknowledge acknowledge, Throwable throwable) -> {
         try {
            if (acknowledge != null) {
            //如果请求成功，则取消 pendingSlotRequest，并更新 slot 状态 PENDING -> ALLOCATED
               updateSlot(slotId, allocationId, pendingSlotRequest.getJobId());
            } else {
               if (throwable instanceof SlotOccupiedException) {
               //这个 slot 已经被占用了，更新状态
                  SlotOccupiedException exception = (SlotOccupiedException) throwable;
                  updateSlot(slotId, exception.getAllocationId(), exception.getJobId());
               } else {
               //请求失败，将 pendingSlotRequest 从 TaskManagerSlot 中移除
                  removeSlotRequestFromSlot(slotId, allocationId);
               }

               if (!(throwable instanceof CancellationException)) {
               //slot request 请求失败，会进行重试
                  handleFailedSlotRequest(slotId, allocationId, throwable);
               } else {
               //主动取消
                  LOG.debug("Slot allocation request {} has been cancelled.", allocationId, throwable);
               }
            }
         } catch (Exception e) {
            LOG.error("Error while completing the slot allocation.", e);
         }
      },
      mainThreadExecutor);
}
```

请求slot调用TaskExecutor 的requestSlot方法，由TaskExecutor 直接向jobManager提供slot

```
public CompletableFuture<Acknowledge> requestSlot(
   final SlotID slotId,
   final JobID jobId,
   final AllocationID allocationId,
   final ResourceProfile resourceProfile,
   final String targetAddress,
   final ResourceManagerId resourceManagerId,
   final Time timeout) {

   try {
   //判断发送请求的 RM 是否是当前 TaskExecutor 注册的
      if (!isConnectedToResourceManager(resourceManagerId)) {
         final String message = String.format("TaskManager is not connected to the resource manager %s.", resourceManagerId);
         log.debug(message);
         throw new TaskManagerException(message);
      }

      if (taskSlotTable.isSlotFree(slotId.getSlotNumber())) {
      //如果 slot 是 Free 状态，则分配 slot
         if (taskSlotTable.allocateSlot(slotId.getSlotNumber(), jobId, allocationId, resourceProfile, taskManagerConfiguration.getTimeout())) {
            log.info("Allocated slot for {}.", allocationId);
         } else {
            log.info("Could not allocate slot for {}.", allocationId);
            throw new SlotAllocationException("Could not allocate slot.");
         }
      } else if (!taskSlotTable.isAllocated(slotId.getSlotNumber(), jobId, allocationId)) {
      //如果 slot 已经被分配了，则抛出异常
         final String message = "The slot " + slotId + " has already been allocated for a different job.";

         log.info(message);

         final AllocationID allocationID = taskSlotTable.getCurrentAllocation(slotId.getSlotNumber());
         throw new SlotOccupiedException(message, allocationID, taskSlotTable.getOwningJob(allocationID));
      }

//将分配的 slot 提供给发送请求的 JobManager
      if (jobManagerTable.contains(jobId)) {
      //如果和对应的 JobManager 已经建立了连接，则向 JobManager 提供 slot
         offerSlotsToJobManager(jobId);
      } else {
         try {
         //否则，先和JobManager 建立连接，连接建立后会调用 offerSlotsToJobManager(jobId) 方法
            jobLeaderService.addJob(jobId, targetAddress);
         } catch (Exception e) {
            // free the allocated slot
            try {
               taskSlotTable.freeSlot(allocationId);
            } catch (SlotNotFoundException slotNotFoundException) {
               // slot no longer existent, this should actually never happen, because we've
               // just allocated the slot. So let's fail hard in this case!
               onFatalError(slotNotFoundException);
            }

            // release local state under the allocation id.
            localStateStoresManager.releaseLocalStateForAllocationId(allocationId);

            // sanity check
            if (!taskSlotTable.isSlotFree(slotId.getSlotNumber())) {
               onFatalError(new Exception("Could not free slot " + slotId));
            }

            throw new SlotAllocationException("Could not add job to job leader service.", e);
         }
      }
   } catch (TaskManagerException taskManagerException) {
      return FutureUtils.completedExceptionally(taskManagerException);
   }

   return CompletableFuture.completedFuture(Acknowledge.get());
}
```

在 Slot 被分配给之后，TaskExecutor 需要将对应的 slot 提供给 JobManager，此时分两种情况，如果请求资源的jobmanager还没有和TaskExecutor 建立连接，则先将TaskExecutor 与jobmanager的leader节点建立连接，并注册TaskExecutor 。然后再通过offerSlotsToJobManager提供slot。如果已经存在连接则直接调用offerSlotsToJobManager

建立连接过程如下：通过JobLeaderService的addJob，由highAvailabilityServices选取该job的JobManager的Leader节点，并启动JobManagerLeaderListener主节点的监听。JobLeaderService是用来管理JobManager的leader节点的服务。

```
public void addJob(final JobID jobId, final String defaultTargetAddress) throws Exception {
   Preconditions.checkState(JobLeaderService.State.STARTED == state, "The service is currently not running.");

   LOG.info("Add job {} for job leader monitoring.", jobId);

   final LeaderRetrievalService leaderRetrievalService = highAvailabilityServices.getJobManagerLeaderRetriever(
      jobId,
      defaultTargetAddress);

   JobLeaderService.JobManagerLeaderListener jobManagerLeaderListener = new JobManagerLeaderListener(jobId);

   final Tuple2<LeaderRetrievalService, JobManagerLeaderListener> oldEntry = jobLeaderServices.put(jobId, Tuple2.of(leaderRetrievalService, jobManagerLeaderListener));

   if (oldEntry != null) {
      oldEntry.f0.stop();
      oldEntry.f1.stop();
   }

   leaderRetrievalService.start(jobManagerLeaderListener);
}
```

主节点选取成功，回调JobManagerLeaderListener的notifyLeaderAddress方法告知JobManager主节点地址

```
@Override
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

rpcConnection.start

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

通过startRegistration最终进入JobLeaderService的invokeRegistration，调用JobMaster的registerTaskManager注册taskManager

```
protected CompletableFuture<RegistrationResponse> invokeRegistration(
      JobMasterGateway gateway,
      JobMasterId jobMasterId,
      long timeoutMillis) throws Exception {
   return gateway.registerTaskManager(taskManagerRpcAddress, taskManagerLocation, Time.milliseconds(timeoutMillis));
}
```

```
public CompletableFuture<RegistrationResponse> registerTaskManager(
      final String taskManagerRpcAddress,
      final TaskManagerLocation taskManagerLocation,
      final Time timeout) {

   final ResourceID taskManagerId = taskManagerLocation.getResourceID();

   if (registeredTaskManagers.containsKey(taskManagerId)) {
      final RegistrationResponse response = new JMTMRegistrationSuccess(resourceId);
      return CompletableFuture.completedFuture(response);
   } else {
      return getRpcService()
         .connect(taskManagerRpcAddress, TaskExecutorGateway.class)
         .handleAsync(
            (TaskExecutorGateway taskExecutorGateway, Throwable throwable) -> {
               if (throwable != null) {
                  return new RegistrationResponse.Decline(throwable.getMessage());
               }

               slotPool.registerTaskManager(taskManagerId);
               registeredTaskManagers.put(taskManagerId, Tuple2.of(taskManagerLocation, taskExecutorGateway));

               // monitor the task manager as heartbeat target
               taskManagerHeartbeatManager.monitorTarget(taskManagerId, new HeartbeatTarget<AllocatedSlotReport>() {
                  @Override
                  public void receiveHeartbeat(ResourceID resourceID, AllocatedSlotReport payload) {
                     // the task manager will not request heartbeat, so this method will never be called currently
                  }

                  @Override
                  public void requestHeartbeat(ResourceID resourceID, AllocatedSlotReport allocatedSlotReport) {
                     taskExecutorGateway.heartbeatFromJobManager(resourceID, allocatedSlotReport);
                  }
               });

               return new JMTMRegistrationSuccess(resourceId);
            },
            getMainThreadExecutor());
   }
}
```

而通过 offerSlotsToJobManager(jobId)方法来实现slot分配给JobManager：

```
private void offerSlotsToJobManager(final JobID jobId) {
   final JobManagerConnection jobManagerConnection = jobManagerTable.get(jobId);

   if (jobManagerConnection == null) {
      log.debug("There is no job manager connection to the leader of job {}.", jobId);
   } else {
      if (taskSlotTable.hasAllocatedSlots(jobId)) {
         log.info("Offer reserved slots to the leader of job {}.", jobId);

         final JobMasterGateway jobMasterGateway = jobManagerConnection.getJobManagerGateway();
       //获取分配给当前 Job 的 slot，这里只会取得状态为 allocated 的 slot
         final Iterator<TaskSlot<Task>> reservedSlotsIterator = taskSlotTable.getAllocatedSlots(jobId);
         final JobMasterId jobMasterId = jobManagerConnection.getJobMasterId();

         final Collection<SlotOffer> reservedSlots = new HashSet<>(2);

         while (reservedSlotsIterator.hasNext()) {
         //SlotOffer请求类，用于向jobmanager提供slot
            SlotOffer offer = reservedSlotsIterator.next().generateSlotOffer();
            reservedSlots.add(offer);
         }

//通过 RPC 调用，将slot提供给 JobMaster
         CompletableFuture<Collection<SlotOffer>> acceptedSlotsFuture = jobMasterGateway.offerSlots(
            getResourceID(),
            reservedSlots,
            taskManagerConfiguration.getTimeout());

         acceptedSlotsFuture.whenCompleteAsync(
            handleAcceptedSlotOffers(jobId, jobMasterGateway, jobMasterId, reservedSlots),
            getMainThreadExecutor());
      } else {
         log.debug("There are no unassigned slots for the job {}.", jobId);
      }
   }
}
```

```
private BiConsumer<Iterable<SlotOffer>, Throwable> handleAcceptedSlotOffers(JobID jobId, JobMasterGateway jobMasterGateway, JobMasterId jobMasterId, Collection<SlotOffer> offeredSlots) {
   return (Iterable<SlotOffer> acceptedSlots, Throwable throwable) -> {
      if (throwable != null) {
      //超时，则重试
         if (throwable instanceof TimeoutException) {
            log.info("Slot offering to JobManager did not finish in time. Retrying the slot offering.");
            // We ran into a timeout. Try again.
            offerSlotsToJobManager(jobId);
         } else {
            log.warn("Slot offering to JobManager failed. Freeing the slots " +
               "and returning them to the ResourceManager.", throwable);

            // We encountered an exception. Free the slots and return them to the RM.
            // 发生异常，则释放所有的 slot
            for (SlotOffer reservedSlot: offeredSlots) {
               freeSlotInternal(reservedSlot.getAllocationId(), throwable);
            }
         }
      } else {
      //调用成功
         // check if the response is still valid
         if (isJobManagerConnectionValid(jobId, jobMasterId)) {
            // mark accepted slots active
            //对于被 JobMaster 确认接受的 slot， 标记为 Active 状态
            for (SlotOffer acceptedSlot : acceptedSlots) {
               try {
                  if (!taskSlotTable.markSlotActive(acceptedSlot.getAllocationId())) {
                     // the slot is either free or releasing at the moment
                     final String message = "Could not mark slot " + jobId + " active.";
                     log.debug(message);
                     jobMasterGateway.failSlot(
                        getResourceID(),
                        acceptedSlot.getAllocationId(),
                        new FlinkException(message));
                  }
               } catch (SlotNotFoundException e) {
                  final String message = "Could not mark slot " + jobId + " active.";
                  jobMasterGateway.failSlot(
                     getResourceID(),
                     acceptedSlot.getAllocationId(),
                     new FlinkException(message));
               }

               offeredSlots.remove(acceptedSlot);
            }

            final Exception e = new Exception("The slot was rejected by the JobManager.");

//释放剩余没有被接受的 slot
            for (SlotOffer rejectedSlot : offeredSlots) {
               freeSlotInternal(rejectedSlot.getAllocationId(), e);
            }
         } else {
            // discard the response since there is a new leader for the job
            log.debug("Discard offer slot response since there is a new leader " +
               "for the job {}.", jobId);
         }
      }
   };
}
```

通过 freeSlotInternal(AllocationID, Throwable)方法， 释放和 `AllocationID` 关联的 slot：

```
private void freeSlotInternal(AllocationID allocationId, Throwable cause) {
   checkNotNull(allocationId);

   log.debug("Free slot with allocation id {} because: {}", allocationId, cause.getMessage());

   try {
      final JobID jobId = taskSlotTable.getOwningJob(allocationId);
//尝试释放 allocationId 绑定的 slot
      final int slotIndex = taskSlotTable.freeSlot(allocationId, cause);

      if (slotIndex != -1) {
//成功释放 slot
         if (isConnectedToResourceManager()) {
         //告知 ResourceManager 当前 slot 可用
            // the slot was freed. Tell the RM about it
            ResourceManagerGateway resourceManagerGateway = establishedResourceManagerConnection.getResourceManagerGateway();

            resourceManagerGateway.notifySlotAvailable(
               establishedResourceManagerConnection.getTaskExecutorRegistrationId(),
               new SlotID(getResourceID(), slotIndex),
               allocationId);
         }

         if (jobId != null) {
        // 如果和 allocationID 绑定的 Job 已经没有分配的 slot 了，那么可以断开和 JobMaster 的连接了
            closeJobManagerConnectionIfNoAllocatedResources(jobId);
         }
      }
   } catch (SlotNotFoundException e) {
      log.debug("Could not free slot for allocation id {}.", allocationId, e);
   }

   localStateStoresManager.releaseLocalStateForAllocationId(allocationId);
}
```

TaskExecutor 的 requestSlot是通过TaskSlotTableImpl的allocateSlot和freeSlot方法来分配和释放slot资源。TaskSlotTableImpl是TaskSlotTable的实现类。

allocateSlot新建TaskSlot类并加入slot的map和已分配slot的map已经每个job的slot的map，freeSlot从各个map中移除TaskSlot

```
public boolean allocateSlot(
      int index,
      JobID jobId,
      AllocationID allocationId,
      ResourceProfile resourceProfile,
      Time slotTimeout) {
   checkRunning();

   Preconditions.checkArgument(index < numberSlots);

   TaskSlot<T> taskSlot = allocatedSlots.get(allocationId);
   if (taskSlot != null) {
      LOG.info("Allocation ID {} is already allocated in {}.", allocationId, taskSlot);
      return false;
   }

   if (taskSlots.containsKey(index)) {
      TaskSlot<T> duplicatedTaskSlot = taskSlots.get(index);
      LOG.info("Slot with index {} already exist, with resource profile {}, job id {} and allocation id {}.",
         index,
         duplicatedTaskSlot.getResourceProfile(),
         duplicatedTaskSlot.getJobId(),
         duplicatedTaskSlot.getAllocationId());
      return duplicatedTaskSlot.getJobId().equals(jobId) &&
         duplicatedTaskSlot.getAllocationId().equals(allocationId);
   } else if (allocatedSlots.containsKey(allocationId)) {
      return true;
   }

   resourceProfile = index >= 0 ? defaultSlotResourceProfile : resourceProfile;

//ResourceBudgetManager分配资源ResourceProfile，ResourceProfile是cpu，内存等指标，资源不够则报错
   if (!budgetManager.reserve(resourceProfile)) {
      LOG.info("Cannot allocate the requested resources. Trying to allocate {}, "
            + "while the currently remaining available resources are {}, total is {}.",
         resourceProfile,
         budgetManager.getAvailableBudget(),
         budgetManager.getTotalBudget());
      return false;
   }

//新建TaskSlot
   taskSlot = new TaskSlot<>(index, resourceProfile, memoryPageSize, jobId, allocationId);
   if (index >= 0) {
      taskSlots.put(index, taskSlot);
   }

   // update the allocation id to task slot map
   allocatedSlots.put(allocationId, taskSlot);

   // register a timeout for this slot since it's in state allocated
   timerService.registerTimeout(allocationId, slotTimeout.getSize(), slotTimeout.getUnit());

   // add this slot to the set of job slots
   Set<AllocationID> slots = slotsPerJob.get(jobId);

   if (slots == null) {
      slots = new HashSet<>(4);
      slotsPerJob.put(jobId, slots);
   }

   slots.add(allocationId);

   return true;
}
```

```
public int freeSlot(AllocationID allocationId, Throwable cause) throws SlotNotFoundException {
   checkStarted();

   TaskSlot<T> taskSlot = getTaskSlot(allocationId);

   if (taskSlot != null) {
      return freeSlotInternal(taskSlot, cause).isDone() ? taskSlot.getIndex() : -1;
   } else {
      throw new SlotNotFoundException(allocationId);
   }
}

private CompletableFuture<Void> freeSlotInternal(TaskSlot<T> taskSlot, Throwable cause) {
   AllocationID allocationId = taskSlot.getAllocationId();

   if (LOG.isDebugEnabled()) {
      LOG.debug("Free slot {}.", taskSlot, cause);
   } else {
      LOG.info("Free slot {}.", taskSlot);
   }

   if (taskSlot.isEmpty()) {
      // remove the allocation id to task slot mapping
      allocatedSlots.remove(allocationId);

      // unregister a potential timeout
      timerService.unregisterTimeout(allocationId);

      JobID jobId = taskSlot.getJobId();
      Set<AllocationID> slots = slotsPerJob.get(jobId);

      if (slots == null) {
         throw new IllegalStateException("There are no more slots allocated for the job " + jobId +
            ". This indicates a programming bug.");
      }

      slots.remove(allocationId);

      if (slots.isEmpty()) {
         slotsPerJob.remove(jobId);
      }

      taskSlots.remove(taskSlot.getIndex());
      //ResourceBudgetManager释放资源ResourceProfile
      budgetManager.release(taskSlot.getResourceProfile());
   }
   return taskSlot.closeAsync(cause);
}
```

然后来看下SlotManagerImpl的updateSlot更新Slot信息，updateSlot有两种情况，1，上报slot消息的taskExecutor已经注册，在reportSlotStatus方法中被调用，2，registerSlot时候被调用，slot已经被分配或没有匹配的 PendingTaskManagerSlot

```
private boolean updateSlot(SlotID slotId, AllocationID allocationId, JobID jobId) {
   final TaskManagerSlot slot = slots.get(slotId);

   if (slot != null) {
      final TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(slot.getInstanceId());

      if (taskManagerRegistration != null) {
         updateSlotState(slot, taskManagerRegistration, allocationId, jobId);

         return true;
      } else {
         throw new IllegalStateException("Trying to update a slot from a TaskManager " +
            slot.getInstanceId() + " which has not been registered.");
      }
   } else {
      LOG.debug("Trying to update unknown slot with slot id {}.", slotId);

      return false;
   }
}
```

主要是调用updateSlotState方法，更新TaskManagerSlot的状态，有ALLOCATED，PENDING，FREE三种状态。

```
private void updateSlotState(
      TaskManagerSlot slot,
      TaskManagerRegistration taskManagerRegistration,
      @Nullable AllocationID allocationId,
      @Nullable JobID jobId) {
   if (null != allocationId) {
      switch (slot.getState()) {
         case PENDING:
            // we have a pending slot request --> check whether we have to reject it
            PendingSlotRequest pendingSlotRequest = slot.getAssignedSlotRequest();

            if (Objects.equals(pendingSlotRequest.getAllocationId(), allocationId)) {
               // we can cancel the slot request because it has been fulfilled
               cancelPendingSlotRequest(pendingSlotRequest);

               // remove the pending slot request, since it has been completed
               pendingSlotRequests.remove(pendingSlotRequest.getAllocationId());

               slot.completeAllocation(allocationId, jobId);
            } else {
               // we first have to free the slot in order to set a new allocationId
               slot.clearPendingSlotRequest();
               // set the allocation id such that the slot won't be considered for the pending slot request
               slot.updateAllocation(allocationId, jobId);

               // remove the pending request if any as it has been assigned
               final PendingSlotRequest actualPendingSlotRequest = pendingSlotRequests.remove(allocationId);

               if (actualPendingSlotRequest != null) {
                  cancelPendingSlotRequest(actualPendingSlotRequest);
               }

               // this will try to find a new slot for the request
               rejectPendingSlotRequest(
                  pendingSlotRequest,
                  new Exception("Task manager reported slot " + slot.getSlotId() + " being already allocated."));
            }

            taskManagerRegistration.occupySlot();
            break;
         case ALLOCATED:
            if (!Objects.equals(allocationId, slot.getAllocationId())) {
               slot.freeSlot();
               slot.updateAllocation(allocationId, jobId);
            }
            break;
         case FREE:
            // the slot is currently free --> it is stored in freeSlots
            freeSlots.remove(slot.getSlotId());
            slot.updateAllocation(allocationId, jobId);
            taskManagerRegistration.occupySlot();
            break;
      }

      fulfilledSlotRequests.put(allocationId, slot.getSlotId());
   } else {
      // no allocation reported
      switch (slot.getState()) {
         case FREE:
            handleFreeSlot(slot);
            break;
         case PENDING:
            // don't do anything because we still have a pending slot request
            break;
         case ALLOCATED:
            AllocationID oldAllocation = slot.getAllocationId();
            slot.freeSlot();
            fulfilledSlotRequests.remove(oldAllocation);
            taskManagerRegistration.freeSlot();

            handleFreeSlot(slot);
            break;
      }
   }
}
```

最后看下TaskSlot，这是TaskManager对slot的封装类，有Releasing，Allocated，Active，Free四种状态，初始状态为Allocated

```
public class TaskSlot<T extends TaskSlotPayload> implements AutoCloseableAsync {
   private static final Logger LOG = LoggerFactory.getLogger(TaskSlot.class);

   /** Index of the task slot. */
   private final int index;

   /** Resource characteristics for this slot. */
   private final ResourceProfile resourceProfile;

   /** Tasks running in this slot. */
   private final Map<ExecutionAttemptID, T> tasks;

   private final MemoryManager memoryManager;

   /** State of this slot. */
   private TaskSlotState state;

   /** Job id to which the slot has been allocated. */
   private final JobID jobId;

   /** Allocation id of this slot. */
   private final AllocationID allocationId;

   /** The closing future is completed when the slot is freed and closed. */
   private final CompletableFuture<Void> closingFuture;
```

TaskSlot可以管理task

```
public boolean add(T task) {
   // Check that this slot has been assigned to the job sending this task
   Preconditions.checkArgument(task.getJobID().equals(jobId), "The task's job id does not match the " +
      "job id for which the slot has been allocated.");
   Preconditions.checkArgument(task.getAllocationId().equals(allocationId), "The task's allocation " +
      "id does not match the allocation id for which the slot has been allocated.");
   Preconditions.checkState(TaskSlotState.ACTIVE == state, "The task slot is not in state active.");

   T oldTask = tasks.put(task.getExecutionId(), task);

   if (oldTask != null) {
      tasks.put(task.getExecutionId(), oldTask);
      return false;
   } else {
      return true;
   }
}
```

已上是TaskManager向Resoucemanager上报slot状态的过程，下面看一下TaskManager和job manager的交互过程。

在前文中提到，在 Slot 被分配给之后，TaskExecutor 需要将对应的 slot 提供给 JobManager，通过offerSlotsToJobManager方法实现，其中调用了jobMasterGateway的offerSlots,jobMasterGateway继承自FencedRpcGateway。

```
CompletableFuture<Collection<SlotOffer>> acceptedSlotsFuture = jobMasterGateway.offerSlots(
   getResourceID(),
   reservedSlots,
   taskManagerConfiguration.getTimeout());
```

JobMasterGateway实现类为JobMaster,进入JobMaster的offerSlots

```
public CompletableFuture<Collection<SlotOffer>> offerSlots(
      final ResourceID taskManagerId,
      final Collection<SlotOffer> slots,
      final Time timeout) {

   Tuple2<TaskManagerLocation, TaskExecutorGateway> taskManager = registeredTaskManagers.get(taskManagerId);

   if (taskManager == null) {
      return FutureUtils.completedExceptionally(new Exception("Unknown TaskManager " + taskManagerId));
   }

   final TaskManagerLocation taskManagerLocation = taskManager.f0;
   final TaskExecutorGateway taskExecutorGateway = taskManager.f1;

//建立TaskManager的Gateway
   final RpcTaskManagerGateway rpcTaskManagerGateway = new RpcTaskManagerGateway(taskExecutorGateway, getFencingToken());

   return CompletableFuture.completedFuture(
      slotPool.offerSlots(
         taskManagerLocation,
         rpcTaskManagerGateway,
         slots));
}
```

JobMaster里管理slot的类是SlotPool，进入SlotPool的offerSlots

```
public Collection<SlotOffer> offerSlots(
      TaskManagerLocation taskManagerLocation,
      TaskManagerGateway taskManagerGateway,
      Collection<SlotOffer> offers) {

   ArrayList<SlotOffer> result = new ArrayList<>(offers.size());
//对每个SlotOffer请求依此调用offerSlot
   for (SlotOffer offer : offers) {
      if (offerSlot(
         taskManagerLocation,
         taskManagerGateway,
         offer)) {

         result.add(offer);
      }
   }

   return result;
}
```

```
boolean offerSlot(
      final TaskManagerLocation taskManagerLocation,
      final TaskManagerGateway taskManagerGateway,
      final SlotOffer slotOffer) {

   componentMainThreadExecutor.assertRunningInMainThread();

   // check if this TaskManager is valid
   final ResourceID resourceID = taskManagerLocation.getResourceID();
   final AllocationID allocationID = slotOffer.getAllocationId();

   if (!registeredTaskManagers.contains(resourceID)) {
      log.debug("Received outdated slot offering [{}] from unregistered TaskManager: {}",
            slotOffer.getAllocationId(), taskManagerLocation);
      return false;
   }

   // check whether we have already using this slot
   AllocatedSlot existingSlot;
   //是否已接收该slot
   if ((existingSlot = allocatedSlots.get(allocationID)) != null ||
      (existingSlot = availableSlots.get(allocationID)) != null) {

      // we need to figure out if this is a repeated offer for the exact same slot,
      // or another offer that comes from a different TaskManager after the ResourceManager
      // re-tried the request

      // we write this in terms of comparing slot IDs, because the Slot IDs are the identifiers of
      // the actual slots on the TaskManagers
      // Note: The slotOffer should have the SlotID
      final SlotID existingSlotId = existingSlot.getSlotId();
      final SlotID newSlotId = new SlotID(taskManagerLocation.getResourceID(), slotOffer.getSlotIndex());

      if (existingSlotId.equals(newSlotId)) {
      //发送了重复的Slot
         log.info("Received repeated offer for slot [{}]. Ignoring.", allocationID);

         // return true here so that the sender will get a positive acknowledgement to the retry
         // and mark the offering as a success
         return true;
      } else {
      //jobmaster需要的slot已经被其他task executor提供的slot满足，则拒绝该slot，该slot最终会释放。
         // the allocation has been fulfilled by another slot, reject the offer so the task executor
         // will offer the slot to the resource manager
         return false;
      }
   }

//创建AllocatedSlot，AllocatedSlot是jobmanager里对slot的封装。通过AllocationID 区分
   final AllocatedSlot allocatedSlot = new AllocatedSlot(
      allocationID,
      taskManagerLocation,
      slotOffer.getSlotIndex(),
      slotOffer.getResourceProfile(),
      taskManagerGateway);

   // check whether we have request waiting for this slot
   PendingRequest pendingRequest = pendingRequests.removeKeyB(allocationID);
   // 检查是否有一个 request 和 这个 AllocationID 关联
   if (pendingRequest != null) {
      // we were waiting for this!
      //有一个pending request 正在等待这个 slot
      allocatedSlots.add(pendingRequest.getSlotRequestId(), allocatedSlot);

//尝试去完成那个等待的请求
      if (!pendingRequest.getAllocatedSlotFuture().complete(allocatedSlot)) {
         // we could not complete the pending slot future --> try to fulfill another pending request
         //尝试失败
         allocatedSlots.remove(pendingRequest.getSlotRequestId());
         //尝试去满足其他在等待的请求
         tryFulfillSlotRequestOrMakeAvailable(allocatedSlot);
      } else {
         log.debug("Fulfilled slot request [{}] with allocated slot [{}].", pendingRequest.getSlotRequestId(), allocationID);
      }
   }
   else {
   //没有请求在等待这个slot，可能请求已经被满足了，尝试去满足其他在等待的请求
      // we were actually not waiting for this:
      //   - could be that this request had been fulfilled
      //   - we are receiving the slots from TaskManagers after becoming leaders
      tryFulfillSlotRequestOrMakeAvailable(allocatedSlot);
   }

   // we accepted the request in any case. slot will be released after it idled for
   // too long and timed out
   return true;
}
```

一旦有新的可用的 `AllocatedSlot` 的时候，`SlotPoolImpl` 会尝试用这个 `AllocatedSlot` 去提前满足其他还在等待响应的请求：

```
private void tryFulfillSlotRequestOrMakeAvailable(AllocatedSlot allocatedSlot) {
   Preconditions.checkState(!allocatedSlot.isUsed(), "Provided slot is still in use.");

/查找和当前 AllocatedSlot 的计算资源相匹配的还在等待的请求
   final PendingRequest pendingRequest = pollMatchingPendingRequest(allocatedSlot);

   if (pendingRequest != null) {
   //如果有匹配的请求，那么将 AllocatedSlot 分配给等待的请求
      log.debug("Fulfilling pending slot request [{}] early with returned slot [{}]",
         pendingRequest.getSlotRequestId(), allocatedSlot.getAllocationId());

      allocatedSlots.add(pendingRequest.getSlotRequestId(), allocatedSlot);
      pendingRequest.getAllocatedSlotFuture().complete(allocatedSlot);
   } else {
   //如果没有，那么这个 AllocatedSlot 变成 available 的
      log.debug("Adding returned slot [{}] to available slots", allocatedSlot.getAllocationId());
      availableSlots.add(allocatedSlot, clock.relativeTimeMillis());
   }
}

private PendingRequest pollMatchingPendingRequest(final AllocatedSlot slot) {
   final ResourceProfile slotResources = slot.getResourceProfile();

   // try the requests sent to the resource manager first
   for (PendingRequest request : pendingRequests.values()) {
      if (slotResources.isMatching(request.getResourceProfile())) {
         pendingRequests.removeKeyA(request.getSlotRequestId());
         return request;
      }
   }

   // try the requests waiting for a resource manager connection next
   for (PendingRequest request : waitingForResourceManager.values()) {
      if (slotResources.isMatching(request.getResourceProfile())) {
         waitingForResourceManager.remove(request.getSlotRequestId());
         return request;
      }
   }

   // no request pending, or no request matches
   return null;
}
```

`slotPool` 启动的时候会开启一个定时调度的任务，周期性地检查空闲的 slot，如果 slot 空闲时间过长，会将该 slot 归还给 TaskManager

```
public void start(
   @Nonnull JobMasterId jobMasterId,
   @Nonnull String newJobManagerAddress,
   @Nonnull ComponentMainThreadExecutor componentMainThreadExecutor) throws Exception {

   this.jobMasterId = jobMasterId;
   this.jobManagerAddress = newJobManagerAddress;
   this.componentMainThreadExecutor = componentMainThreadExecutor;

//检查空闲slot
   scheduleRunAsync(this::checkIdleSlot, idleSlotTimeout);
   scheduleRunAsync(this::checkBatchSlotTimeout, batchSlotTimeout);

   if (log.isDebugEnabled()) {
      scheduleRunAsync(this::scheduledLogStatus, STATUS_LOG_INTERVAL_MS, TimeUnit.MILLISECONDS);
   }
}
```

```
protected void checkIdleSlot() {

   // The timestamp in SlotAndTimestamp is relative
   final long currentRelativeTimeMillis = clock.relativeTimeMillis();

   final List<AllocatedSlot> expiredSlots = new ArrayList<>(availableSlots.size());

   for (SlotAndTimestamp slotAndTimestamp : availableSlots.availableSlots.values()) {
      if (currentRelativeTimeMillis - slotAndTimestamp.timestamp > idleSlotTimeout.toMilliseconds()) {
         expiredSlots.add(slotAndTimestamp.slot);
      }
   }

   final FlinkException cause = new FlinkException("Releasing idle slot.");

   for (AllocatedSlot expiredSlot : expiredSlots) {
      final AllocationID allocationID = expiredSlot.getAllocationId();
      if (availableSlots.tryRemove(allocationID) != null) {
//将空闲的 slot 归还给 TaskManager，调用TaskExecutor的freeSlot
         log.info("Releasing idle slot [{}].", allocationID);
         final CompletableFuture<Acknowledge> freeSlotFuture = expiredSlot.getTaskManagerGateway().freeSlot(
            allocationID,
            cause,
            rpcTimeout);

         FutureUtils.whenCompleteAsyncIfNotDone(
            freeSlotFuture,
            componentMainThreadExecutor,
            (Acknowledge ignored, Throwable throwable) -> {
               if (throwable != null) {
                  // The slot status will be synced to task manager in next heartbeat.
                  log.debug("Releasing slot [{}] of registered TaskExecutor {} failed. Discarding slot.",
                           allocationID, expiredSlot.getTaskManagerId(), throwable);
               }
            });
      }
   }

   scheduleRunAsync(this::checkIdleSlot, idleSlotTimeout);
}
```

SlotPool通过requestNewAllocatedSlot请求Slot，根据所需的资源大小创建PendingRequest类用于申请slot

```
public CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(
			@Nonnull SlotRequestId slotRequestId,
			@Nonnull ResourceProfile resourceProfile,
			Time timeout) {

		componentMainThreadExecutor.assertRunningInMainThread();

		final PendingRequest pendingRequest = PendingRequest.createStreamingRequest(slotRequestId, resourceProfile);

		// register request timeout
		FutureUtils
			.orTimeout(
				pendingRequest.getAllocatedSlotFuture(),
				timeout.toMilliseconds(),
				TimeUnit.MILLISECONDS,
				componentMainThreadExecutor)
			.whenComplete(
				(AllocatedSlot ignored, Throwable throwable) -> {
					if (throwable instanceof TimeoutException) {
						timeoutPendingSlotRequest(slotRequestId);
					}
				});

		return requestNewAllocatedSlotInternal(pendingRequest)
			.thenApply((Function.identity()));
	}

	@Nonnull
	@Override
	public CompletableFuture<PhysicalSlot> requestNewAllocatedBatchSlot(
		@Nonnull SlotRequestId slotRequestId,
		@Nonnull ResourceProfile resourceProfile) {

		componentMainThreadExecutor.assertRunningInMainThread();

		final PendingRequest pendingRequest = PendingRequest.createBatchRequest(slotRequestId, resourceProfile);

		return requestNewAllocatedSlotInternal(pendingRequest)
			.thenApply(Function.identity());
	}
```

最终通过RPC调用ResourceManager的requestSlot，并封装SlotRequest请求类

```
private CompletableFuture<AllocatedSlot> requestNewAllocatedSlotInternal(PendingRequest pendingRequest) {

   if (resourceManagerGateway == null) {
      stashRequestWaitingForResourceManager(pendingRequest);
   } else {
      requestSlotFromResourceManager(resourceManagerGateway, pendingRequest);
   }

   return pendingRequest.getAllocatedSlotFuture();
}

private void requestSlotFromResourceManager(
      final ResourceManagerGateway resourceManagerGateway,
      final PendingRequest pendingRequest) {

   checkNotNull(resourceManagerGateway);
   checkNotNull(pendingRequest);

   log.info("Requesting new slot [{}] and profile {} from resource manager.", pendingRequest.getSlotRequestId(), pendingRequest.getResourceProfile());

   final AllocationID allocationId = new AllocationID();

   pendingRequests.put(pendingRequest.getSlotRequestId(), allocationId, pendingRequest);

   pendingRequest.getAllocatedSlotFuture().whenComplete(
      (AllocatedSlot allocatedSlot, Throwable throwable) -> {
         if (throwable != null || !allocationId.equals(allocatedSlot.getAllocationId())) {
            // cancel the slot request if there is a failure or if the pending request has
            // been completed with another allocated slot
            resourceManagerGateway.cancelSlotRequest(allocationId);
         }
      });

   CompletableFuture<Acknowledge> rmResponse = resourceManagerGateway.requestSlot(
      jobMasterId,
      new SlotRequest(jobId, allocationId, pendingRequest.getResourceProfile(), jobManagerAddress),
      rpcTimeout);

   FutureUtils.whenCompleteAsyncIfNotDone(
      rmResponse,
      componentMainThreadExecutor,
      (Acknowledge ignored, Throwable failure) -> {
         // on failure, fail the request future
         if (failure != null) {
            slotRequestToResourceManagerFailed(pendingRequest.getSlotRequestId(), failure);
         }
      });
}
```

ResourceManager的requestSlot调用SlotManager的registerSlotRequest

```
public CompletableFuture<Acknowledge> requestSlot(
      JobMasterId jobMasterId,
      SlotRequest slotRequest,
      final Time timeout) {

   JobID jobId = slotRequest.getJobId();
   JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.get(jobId);

   if (null != jobManagerRegistration) {
      if (Objects.equals(jobMasterId, jobManagerRegistration.getJobMasterId())) {
         log.info("Request slot with profile {} for job {} with allocation id {}.",
            slotRequest.getResourceProfile(),
            slotRequest.getJobId(),
            slotRequest.getAllocationId());

         try {
            slotManager.registerSlotRequest(slotRequest);
         } catch (ResourceManagerException e) {
            return FutureUtils.completedExceptionally(e);
         }

         return CompletableFuture.completedFuture(Acknowledge.get());
      } else {
         return FutureUtils.completedExceptionally(new ResourceManagerException("The job leader's id " +
            jobManagerRegistration.getJobMasterId() + " does not match the received id " + jobMasterId + '.'));
      }

   } else {
      return FutureUtils.completedExceptionally(new ResourceManagerException("Could not find registered job manager for job " + jobId + '.'));
   }
}
```

SlotManager通过internalRequestSlot查找是否有满足输入的PendingSlotRequest的预留资源，如果有通过fulfillPendingSlotRequestWithPendingTaskManagerSlot直接分配slot，如果没有通过allocateSlot向TaskManager申请slot。allocateSlot与前文逻辑一致

```
public boolean registerSlotRequest(SlotRequest slotRequest) throws ResourceManagerException {
   checkInit();

   if (checkDuplicateRequest(slotRequest.getAllocationId())) {
      LOG.debug("Ignoring a duplicate slot request with allocation id {}.", slotRequest.getAllocationId());

      return false;
   } else {
      PendingSlotRequest pendingSlotRequest = new PendingSlotRequest(slotRequest);

      pendingSlotRequests.put(slotRequest.getAllocationId(), pendingSlotRequest);

      try {
         internalRequestSlot(pendingSlotRequest);
      } catch (ResourceManagerException e) {
         // requesting the slot failed --> remove pending slot request
         pendingSlotRequests.remove(slotRequest.getAllocationId());

         throw new ResourceManagerException("Could not fulfill slot request " + slotRequest.getAllocationId() + '.', e);
      }

      return true;
   }
}
```

```
private void internalRequestSlot(PendingSlotRequest pendingSlotRequest) throws ResourceManagerException {
   final ResourceProfile resourceProfile = pendingSlotRequest.getResourceProfile();

   OptionalConsumer.of(findMatchingSlot(resourceProfile))
      .ifPresent(taskManagerSlot -> allocateSlot(taskManagerSlot, pendingSlotRequest))
      .ifNotPresent(() -> fulfillPendingSlotRequestWithPendingTaskManagerSlot(pendingSlotRequest));
}

private void fulfillPendingSlotRequestWithPendingTaskManagerSlot(PendingSlotRequest pendingSlotRequest) throws ResourceManagerException {
   ResourceProfile resourceProfile = pendingSlotRequest.getResourceProfile();
   Optional<PendingTaskManagerSlot> pendingTaskManagerSlotOptional = findFreeMatchingPendingTaskManagerSlot(resourceProfile);

   if (!pendingTaskManagerSlotOptional.isPresent()) {
      pendingTaskManagerSlotOptional = allocateResource(resourceProfile);
   }

   OptionalConsumer.of(pendingTaskManagerSlotOptional)
      .ifPresent(pendingTaskManagerSlot -> assignPendingTaskManagerSlot(pendingSlotRequest, pendingTaskManagerSlot))
      .ifNotPresent(() -> {
         // request can not be fulfilled by any free slot or pending slot that can be allocated,
         // check whether it can be fulfilled by allocated slots
         if (failUnfulfillableRequest && !isFulfillableByRegisteredSlots(pendingSlotRequest.getResourceProfile())) {
            throw new UnfulfillableSlotRequestException(pendingSlotRequest.getAllocationId(), pendingSlotRequest.getResourceProfile());
         }
      });
}
```