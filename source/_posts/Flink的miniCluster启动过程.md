flink的PipelineExecutor分为远程和本地，以本地环境为例，看下miniCluster启动过程。入口在本地环境LocalExecutor的excute方法里。先根据streamGraph生成JobGraph，再启动MiniCluster，然后将JobGraph提交至MiniCluster。

```
public CompletableFuture<JobClient> execute(Pipeline pipeline, Configuration configuration) throws Exception {

   final JobGraph jobGraph = getJobGraph(pipeline, configuration);
   final MiniCluster miniCluster = startMiniCluster(jobGraph, configuration);
   final MiniClusterClient clusterClient = new MiniClusterClient(configuration, miniCluster);

   CompletableFuture<JobID> jobIdFuture = clusterClient.submitJob(jobGraph);

   ......
}
```

看下startMiniCluster的过程，

```
private MiniCluster startMiniCluster(final JobGraph jobGraph, final Configuration configuration) throws Exception {
   if (!configuration.contains(RestOptions.BIND_PORT)) {
      configuration.setString(RestOptions.BIND_PORT, "0");
   }

//要启动的TaskManager个数，由local.number-taskmanager配置，默认为1
   int numTaskManagers = configuration.getInteger(
         ConfigConstants.LOCAL_NUMBER_TASK_MANAGER,
         ConfigConstants.DEFAULT_LOCAL_NUMBER_TASK_MANAGER);

   // we have to use the maximum parallelism as a default here, otherwise streaming
   // pipelines would not run
   //每个TaskManager的slot的数量，默认最大并行度
   int numSlotsPerTaskManager = configuration.getInteger(
         TaskManagerOptions.NUM_TASK_SLOTS,
         jobGraph.getMaximumParallelism());

//配置参数
   final MiniClusterConfiguration miniClusterConfiguration =
         new MiniClusterConfiguration.Builder()
               .setConfiguration(configuration)
               .setNumTaskManagers(numTaskManagers)
               .setRpcServiceSharing(RpcServiceSharing.SHARED)
               .setNumSlotsPerTaskManager(numSlotsPerTaskManager)
               .build();

//启动集群
   final MiniCluster miniCluster = new MiniCluster(miniClusterConfiguration);
   miniCluster.start();

   configuration.setInteger(RestOptions.PORT, miniCluster.getRestAddress().get().getPort());

   return miniCluster;
}
```

MiniCluster类构造函数，定义了组件个数及超时时间等

```
public MiniCluster(MiniClusterConfiguration miniClusterConfiguration) {
   this.miniClusterConfiguration = checkNotNull(miniClusterConfiguration, "config may not be null");
   //需要启动的服务个数，common + JM + RM + TMs,commonRpcService用来进行一些额外的调用，比如获取metric等
   this.rpcServices = new ArrayList<>(1 + 2 + miniClusterConfiguration.getNumTaskManagers()); // common + JM + RM + TMs
   this.dispatcherResourceManagerComponents = new ArrayList<>(1);

   this.rpcTimeout = miniClusterConfiguration.getRpcTimeout();
   this.terminationFuture = CompletableFuture.completedFuture(null);
   running = false;

   this.taskManagers = new ArrayList<>(miniClusterConfiguration.getNumTaskManagers());
}
```

MiniCluster类的start函数，启动MiniCluster的各个组件：

```
public void start() throws Exception {
   synchronized (lock) {
      checkState(!running, "MiniCluster is already running");

      LOG.info("Starting Flink Mini Cluster");
      LOG.debug("Using configuration {}", miniClusterConfiguration);

      final Configuration configuration = miniClusterConfiguration.getConfiguration();
      final boolean useSingleRpcService = miniClusterConfiguration.getRpcServiceSharing() == RpcServiceSharing.SHARED;

      try {
         initializeIOFormatClasses(configuration);

         LOG.info("Starting Metrics Registry");
         metricRegistry = createMetricRegistry(configuration);

         // bring up all the RPC services
         LOG.info("Starting RPC Service(s)");

//akka的Rpc服务
         AkkaRpcServiceConfiguration akkaRpcServiceConfig = AkkaRpcServiceConfiguration.fromConfiguration(configuration);

         final RpcServiceFactory dispatcherResourceManagreComponentRpcServiceFactory;

//如果只用一个Rpc服务，则所有组件起在一个服务里
         if (useSingleRpcService) {
            // we always need the 'commonRpcService' for auxiliary calls
            commonRpcService = createRpcService(akkaRpcServiceConfig, false, null);
            final CommonRpcServiceFactory commonRpcServiceFactory = new CommonRpcServiceFactory(commonRpcService);
            taskManagerRpcServiceFactory = commonRpcServiceFactory;
            dispatcherResourceManagreComponentRpcServiceFactory = commonRpcServiceFactory;
         } else {
         //不同的组件分开部署
            // we always need the 'commonRpcService' for auxiliary calls
            commonRpcService = createRpcService(akkaRpcServiceConfig, true, null);

            // start a new service per component, possibly with custom bind addresses
            final String jobManagerBindAddress = miniClusterConfiguration.getJobManagerBindAddress();
            final String taskManagerBindAddress = miniClusterConfiguration.getTaskManagerBindAddress();

            dispatcherResourceManagreComponentRpcServiceFactory = new DedicatedRpcServiceFactory(akkaRpcServiceConfig, jobManagerBindAddress);
            taskManagerRpcServiceFactory = new DedicatedRpcServiceFactory(akkaRpcServiceConfig, taskManagerBindAddress);
         }

//metric查询服务
         RpcService metricQueryServiceRpcService = MetricUtils.startMetricsRpcService(
            configuration,
            commonRpcService.getAddress());
         metricRegistry.startQueryService(metricQueryServiceRpcService, null);

         processMetricGroup = MetricUtils.instantiateProcessMetricGroup(
            metricRegistry,
            RpcUtils.getHostname(commonRpcService),
            ConfigurationUtils.getSystemResourceMetricsProbingInterval(configuration));

//创建高可用HA服务
         ioExecutor = Executors.newFixedThreadPool(
            Hardware.getNumberCPUCores(),
            new ExecutorThreadFactory("mini-cluster-io"));
         haServices = createHighAvailabilityServices(configuration, ioExecutor);

//启动blobServer
         blobServer = new BlobServer(configuration, haServices.createBlobStore());
         blobServer.start();

         heartbeatServices = HeartbeatServices.fromConfiguration(configuration);

         blobCacheService = new BlobCacheService(
            configuration, haServices.createBlobStore(), new InetSocketAddress(InetAddress.getLocalHost(), blobServer.getPort())
         );

//启动TaskManager
         startTaskManagers();

         MetricQueryServiceRetriever metricQueryServiceRetriever = new RpcMetricQueryServiceRetriever(metricRegistry.getMetricQueryServiceRpcService());

//创建DispatcherResourceManager
         setupDispatcherResourceManagerComponents(configuration, dispatcherResourceManagreComponentRpcServiceFactory, metricQueryServiceRetriever);

         resourceManagerLeaderRetriever = haServices.getResourceManagerLeaderRetriever();
         dispatcherLeaderRetriever = haServices.getDispatcherLeaderRetriever();
         clusterRestEndpointLeaderRetrievalService = haServices.getClusterRestEndpointLeaderRetriever();

         dispatcherGatewayRetriever = new RpcGatewayRetriever<>(
            commonRpcService,
            DispatcherGateway.class,
            DispatcherId::fromUuid,
            20,
            Time.milliseconds(20L));
         resourceManagerGatewayRetriever = new RpcGatewayRetriever<>(
            commonRpcService,
            ResourceManagerGateway.class,
            ResourceManagerId::fromUuid,
            20,
            Time.milliseconds(20L));
         webMonitorLeaderRetriever = new LeaderRetriever();

//启动resourceManager及dispatcher
         resourceManagerLeaderRetriever.start(resourceManagerGatewayRetriever);
         dispatcherLeaderRetriever.start(dispatcherGatewayRetriever);
         clusterRestEndpointLeaderRetrievalService.start(webMonitorLeaderRetriever);
      }
......
}
```

接下来看下上面过程中的部分服务的启动，首先是创建`HighAvailabilityServices`，根据配置的HA模式来实现不同的HA类，如果没有配置则使用EmbeddedHaServices。

```
protected HighAvailabilityServices createHighAvailabilityServices(Configuration configuration, Executor executor) throws Exception {
   LOG.info("Starting high-availability services");
   return HighAvailabilityServicesUtils.createAvailableOrEmbeddedServices(
      configuration,
      executor);
}

public class HighAvailabilityServicesUtils {

	public static HighAvailabilityServices createAvailableOrEmbeddedServices(
		Configuration config,
		Executor executor) throws Exception {
		HighAvailabilityMode highAvailabilityMode = HighAvailabilityMode.fromConfig(config);

		switch (highAvailabilityMode) {
			case NONE:
				return new EmbeddedHaServices(executor);

			case ZOOKEEPER:
				BlobStoreService blobStoreService = BlobUtils.createBlobStoreFromConfig(config);

				return new ZooKeeperHaServices(
					ZooKeeperUtils.startCuratorFramework(config),
					executor,
					config,
					blobStoreService);

			case FACTORY_CLASS:
				return createCustomHAServices(config, executor);

			default:
				throw new Exception("High availability mode " + highAvailabilityMode + " is not supported.");
		}
	}

```

接着是startTaskManagers，根据配置的numTaskManagers数量调用startTaskExecutor，`TaskManagerRunner#startTaskManager` 会创建一个 `TaskExecutor`, `TaskExecutor` 实现了 `RpcEndpoint`接口。 `TaskExecutor` 需要和 `ResourceManager` 等组件进行通信，可以通过 `HighAvailabilityServices`获得对应的服务地址。

```
private void startTaskManagers() throws Exception {
   final int numTaskManagers = miniClusterConfiguration.getNumTaskManagers();

   LOG.info("Starting {} TaskManger(s)", numTaskManagers);

   for (int i = 0; i < numTaskManagers; i++) {
      startTaskExecutor();
   }
}


void startTaskExecutor() throws Exception {
		synchronized (lock) {
			final Configuration configuration = miniClusterConfiguration.getConfiguration();

			final TaskExecutor taskExecutor = TaskManagerRunner.startTaskManager(
				configuration,
				new ResourceID(UUID.randomUUID().toString()),
				taskManagerRpcServiceFactory.createRpcService(),
				haServices,
				heartbeatServices,
				metricRegistry,
				blobCacheService,
				useLocalCommunication(),
				taskManagerTerminatingFatalErrorHandlerFactory.create(taskManagers.size()));

			taskExecutor.start();
			taskManagers.add(taskExecutor);
		}
	}
```

`TaskManagerRunner#startTaskManager`,创建TaskExecutor

```
public static TaskExecutor startTaskManager(
      Configuration configuration,
      ResourceID resourceID,
      RpcService rpcService,
      HighAvailabilityServices highAvailabilityServices,
      HeartbeatServices heartbeatServices,
      MetricRegistry metricRegistry,
      BlobCacheService blobCacheService,
      boolean localCommunicationOnly,
      FatalErrorHandler fatalErrorHandler) throws Exception {

   ......

   InetAddress remoteAddress = InetAddress.getByName(rpcService.getAddress());

   final TaskExecutorResourceSpec taskExecutorResourceSpec = TaskExecutorResourceUtils.resourceSpecFromConfig(configuration);

   TaskManagerServicesConfiguration taskManagerServicesConfiguration =
      TaskManagerServicesConfiguration.fromConfiguration(
         configuration,
         resourceID,
         remoteAddress,
         localCommunicationOnly,
         taskExecutorResourceSpec);

   Tuple2<TaskManagerMetricGroup, MetricGroup> taskManagerMetricGroup = MetricUtils.instantiateTaskManagerMetricGroup(
      metricRegistry,
      TaskManagerLocation.getHostName(remoteAddress),
      resourceID,
      taskManagerServicesConfiguration.getSystemResourceMetricsProbingInterval());

   TaskManagerServices taskManagerServices = TaskManagerServices.fromConfiguration(
      taskManagerServicesConfiguration,
      taskManagerMetricGroup.f1,
      rpcService.getExecutor()); // TODO replace this later with some dedicated executor for io.

   TaskManagerConfiguration taskManagerConfiguration =
      TaskManagerConfiguration.fromConfiguration(configuration, taskExecutorResourceSpec);

   String metricQueryServiceAddress = metricRegistry.getMetricQueryServiceGatewayRpcAddress();

   return new TaskExecutor(
      rpcService,
      taskManagerConfiguration,
      highAvailabilityServices,
      taskManagerServices,
      heartbeatServices,
      taskManagerMetricGroup.f0,
      metricQueryServiceAddress,
      blobCacheService,
      fatalErrorHandler,
      new TaskExecutorPartitionTrackerImpl(taskManagerServices.getShuffleEnvironment()),
      createBackPressureSampleService(configuration, rpcService.getScheduledExecutor()));
}
```

TaskExecutor实现了RpcEndpoint，创建TaskExecutor之后调用RpcEndpoint的start启动akkaRpcActor

```
public class TaskExecutor extends RpcEndpoint implements TaskExecutorGateway
```

启动akkaRpcActor成功会调用TaskExecutor的回调函数onStart，建立一系列连接

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
	
	
	
	
	
	private final class ResourceManagerLeaderListener implements LeaderRetrievalListener {

		@Override
		public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
			runAsync(
			//与ResourceManager建立连接
				() -> notifyOfNewResourceManagerLeader(
					leaderAddress,
					ResourceManagerId.fromUuidOrNull(leaderSessionID)));
		}

		@Override
		public void handleError(Exception exception) {
			onFatalError(exception);
		}
	}

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

当 `ResourceManagerLeaderListener` 的监听被回调时，`TaskExecutor` 会试图建立和 `ResourceManager` 的连接，连接被封装为 `TaskExecutorToResourceManagerConnection`。一旦获取 `ResourceManager` 的 leader 被确定后，就可以获取到 `ResourceManager` 对应的 RpcGateway， 接下来就可以通过 RPC 调用发起 `ResourceManager#registerTaskExecutor` 注册流程。注册成功后，回调onRegistrationSuccess方法，`TaskExecutor` 向 `ResourceManager` 报告其资源（主要是 slots）情况，并建立连接。

```
private void connectToResourceManager() {
   assert(resourceManagerAddress != null);
   assert(establishedResourceManagerConnection == null);
   assert(resourceManagerConnection == null);

   log.info("Connecting to ResourceManager {}.", resourceManagerAddress);

   final TaskExecutorRegistration taskExecutorRegistration = new TaskExecutorRegistration(
      getAddress(),
      getResourceID(),
      taskManagerLocation.dataPort(),
      hardwareDescription,
      taskManagerConfiguration.getDefaultSlotResourceProfile(),
      taskManagerConfiguration.getTotalResourceProfile()
   );

   resourceManagerConnection =
      new TaskExecutorToResourceManagerConnection(
         log,
         getRpcService(),
         taskManagerConfiguration.getRetryingRegistrationConfiguration(),
         resourceManagerAddress.getAddress(),
         resourceManagerAddress.getResourceManagerId(),
         getMainThreadExecutor(),
         new ResourceManagerRegistrationListener(),
         taskExecutorRegistration);
   resourceManagerConnection.start();
}


resourceManagerConnection.start()方法：
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
	
	
createNewRegistration方法：
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
	

//generateRegistration方法
@Override
	protected RetryingRegistration<ResourceManagerId, ResourceManagerGateway, TaskExecutorRegistrationSuccess> generateRegistration() {
		return new TaskExecutorToResourceManagerConnection.ResourceManagerRegistration(
			log,
			rpcService,
			getTargetAddress(),
			getTargetLeaderId(),
			retryingRegistrationConfiguration,
			taskExecutorRegistration);
	}



TaskExecutorToResourceManagerConnection类内部类：
private static class ResourceManagerRegistration
			extends RetryingRegistration<ResourceManagerId, ResourceManagerGateway, TaskExecutorRegistrationSuccess> {

		private final TaskExecutorRegistration taskExecutorRegistration;

		ResourceManagerRegistration(
				Logger log,
				RpcService rpcService,
				String targetAddress,
				ResourceManagerId resourceManagerId,
				RetryingRegistrationConfiguration retryingRegistrationConfiguration,
				TaskExecutorRegistration taskExecutorRegistration) {

			super(log, rpcService, "ResourceManager", ResourceManagerGateway.class, targetAddress, resourceManagerId, retryingRegistrationConfiguration);
			this.taskExecutorRegistration = taskExecutorRegistration;
		}

		@Override
		protected CompletableFuture<RegistrationResponse> invokeRegistration(
				ResourceManagerGateway resourceManager, ResourceManagerId fencingToken, long timeoutMillis) throws Exception {

			Time timeout = Time.milliseconds(timeoutMillis);
			//register taskExecutor
			return resourceManager.registerTaskExecutor(
				taskExecutorRegistration,
				timeout);
		}
	}




TaskExecutorToResourceManagerConnection类：
//完成上述Registration回调onRegistrationSuccess
@Override
	protected void onRegistrationSuccess(TaskExecutorRegistrationSuccess success) {
		log.info("Successful registration at resource manager {} under registration id {}.",
			getTargetAddress(), success.getRegistrationId());

		registrationListener.onRegistrationSuccess(this, success);
	}
	
	TaskExecutor类内部的ResourceManagerRegistrationListener类：
	public void onRegistrationSuccess(TaskExecutorToResourceManagerConnection connection, TaskExecutorRegistrationSuccess success) {
			final ResourceID resourceManagerId = success.getResourceManagerId();
			final InstanceID taskExecutorRegistrationId = success.getRegistrationId();
			final ClusterInformation clusterInformation = success.getClusterInformation();
			final ResourceManagerGateway resourceManagerGateway = connection.getTargetGateway();

			runAsync(
				() -> {
					// filter out outdated connections
					//noinspection ObjectEquality
					if (resourceManagerConnection == connection) {
						establishResourceManagerConnection(
							resourceManagerGateway,
							resourceManagerId,
							taskExecutorRegistrationId,
							clusterInformation);
					}
				});
		}
		

private void establishResourceManagerConnection(
			ResourceManagerGateway resourceManagerGateway,
			ResourceID resourceManagerResourceId,
			InstanceID taskExecutorRegistrationId,
			ClusterInformation clusterInformation) {
//发送SlotReport
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

		// monitor the resource manager as heartbeat target
		resourceManagerHeartbeatManager.monitorTarget(resourceManagerResourceId, new HeartbeatTarget<TaskExecutorHeartbeatPayload>() {
			@Override
			public void receiveHeartbeat(ResourceID resourceID, TaskExecutorHeartbeatPayload heartbeatPayload) {
				resourceManagerGateway.heartbeatFromTaskManager(resourceID, heartbeatPayload);
			}

			@Override
			public void requestHeartbeat(ResourceID resourceID, TaskExecutorHeartbeatPayload heartbeatPayload) {
				// the TaskManager won't send heartbeat requests to the ResourceManager
			}
		});

		// set the propagated blob server address
		final InetSocketAddress blobServerAddress = new InetSocketAddress(
			clusterInformation.getBlobServerHostname(),
			clusterInformation.getBlobServerPort());

		blobCacheService.setBlobServerAddress(blobServerAddress);

//连接建立
		establishedResourceManagerConnection = new EstablishedResourceManagerConnection(
			resourceManagerGateway,
			resourceManagerResourceId,
			taskExecutorRegistrationId);

		stopRegistrationTimeout();
	}
```

最后，启动完taskmanager进入DispatcherResourceManagerComponent的启动过程

```
private void setupDispatcherResourceManagerComponents(Configuration configuration, RpcServiceFactory dispatcherResourceManagreComponentRpcServiceFactory, MetricQueryServiceRetriever metricQueryServiceRetriever) throws Exception {
   dispatcherResourceManagerComponents.addAll(createDispatcherResourceManagerComponents(
      configuration,
      dispatcherResourceManagreComponentRpcServiceFactory,
      haServices,
      blobServer,
      heartbeatServices,
      metricRegistry,
      metricQueryServiceRetriever,
      new ShutDownFatalErrorHandler()
   ));

   ......
}
```

createDispatcherResourceManagerComponents:

```
protected Collection<? extends DispatcherResourceManagerComponent> createDispatcherResourceManagerComponents(
      Configuration configuration,
      RpcServiceFactory rpcServiceFactory,
      HighAvailabilityServices haServices,
      BlobServer blobServer,
      HeartbeatServices heartbeatServices,
      MetricRegistry metricRegistry,
      MetricQueryServiceRetriever metricQueryServiceRetriever,
      FatalErrorHandler fatalErrorHandler) throws Exception {
   DispatcherResourceManagerComponentFactory dispatcherResourceManagerComponentFactory = createDispatcherResourceManagerComponentFactory();
   return Collections.singleton(
      dispatcherResourceManagerComponentFactory.create(
         configuration,
         ioExecutor,
         rpcServiceFactory.createRpcService(),
         haServices,
         blobServer,
         heartbeatServices,
         metricRegistry,
         new MemoryArchivedExecutionGraphStore(),
         metricQueryServiceRetriever,
         fatalErrorHandler));
}
```

createDispatcherResourceManagerComponentFactory方法，分别得到Dispatcher，ResourceManager及restEndpoint组件的工厂类。分别是SessionDispatcherLeaderProcessFactoryFactory，StandaloneResourceManagerFactory以及SessionRestEndpointFactory

```
private DispatcherResourceManagerComponentFactory createDispatcherResourceManagerComponentFactory() {
   return DefaultDispatcherResourceManagerComponentFactory.createSessionComponentFactory(StandaloneResourceManagerFactory.INSTANCE);
}



public static DefaultDispatcherResourceManagerComponentFactory createSessionComponentFactory(
			ResourceManagerFactory<?> resourceManagerFactory) {
		return new DefaultDispatcherResourceManagerComponentFactory(
			DefaultDispatcherRunnerFactory.createSessionRunner(SessionDispatcherFactory.INSTANCE),
			resourceManagerFactory,
			SessionRestEndpointFactory.INSTANCE);
	}



DefaultDispatcherResourceManagerComponentFactory(
			@Nonnull DispatcherRunnerFactory dispatcherRunnerFactory,
			@Nonnull ResourceManagerFactory<?> resourceManagerFactory,
			@Nonnull RestEndpointFactory<?> restEndpointFactory) {
		this.dispatcherRunnerFactory = dispatcherRunnerFactory;
		this.resourceManagerFactory = resourceManagerFactory;
		this.restEndpointFactory = restEndpointFactory;
	}
```

DefaultDispatcherResourceManagerComponentFactory类create方法创建Dispatcher，ResourceManager及restEndpoint组件

```
public DispatcherResourceManagerComponent create(
      Configuration configuration,
      Executor ioExecutor,
      RpcService rpcService,
      HighAvailabilityServices highAvailabilityServices,
      BlobServer blobServer,
      HeartbeatServices heartbeatServices,
      MetricRegistry metricRegistry,
      ArchivedExecutionGraphStore archivedExecutionGraphStore,
      MetricQueryServiceRetriever metricQueryServiceRetriever,
      FatalErrorHandler fatalErrorHandler) throws Exception {

   LeaderRetrievalService dispatcherLeaderRetrievalService = null;
   LeaderRetrievalService resourceManagerRetrievalService = null;
   WebMonitorEndpoint<?> webMonitorEndpoint = null;
   ResourceManager<?> resourceManager = null;
   ResourceManagerMetricGroup resourceManagerMetricGroup = null;
   DispatcherRunner dispatcherRunner = null;

      dispatcherLeaderRetrievalService = highAvailabilityServices.getDispatcherLeaderRetriever();

      resourceManagerRetrievalService = highAvailabilityServices.getResourceManagerLeaderRetriever();

      final LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever = new RpcGatewayRetriever<>(
         rpcService,
         DispatcherGateway.class,
         DispatcherId::fromUuid,
         10,
         Time.milliseconds(50L));

      final LeaderGatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever = new RpcGatewayRetriever<>(
         rpcService,
         ResourceManagerGateway.class,
         ResourceManagerId::fromUuid,
         10,
         Time.milliseconds(50L));

      final ExecutorService executor = WebMonitorEndpoint.createExecutorService(
         configuration.getInteger(RestOptions.SERVER_NUM_THREADS),
         configuration.getInteger(RestOptions.SERVER_THREAD_PRIORITY),
         "DispatcherRestEndpoint");

      final long updateInterval = configuration.getLong(MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL);
      final MetricFetcher metricFetcher = updateInterval == 0
         ? VoidMetricFetcher.INSTANCE
         : MetricFetcherImpl.fromConfiguration(
            configuration,
            metricQueryServiceRetriever,
            dispatcherGatewayRetriever,
            executor);

      webMonitorEndpoint = restEndpointFactory.createRestEndpoint(
         configuration,
         dispatcherGatewayRetriever,
         resourceManagerGatewayRetriever,
         blobServer,
         executor,
         metricFetcher,
         highAvailabilityServices.getClusterRestEndpointLeaderElectionService(),
         fatalErrorHandler);

      log.debug("Starting Dispatcher REST endpoint.");
      webMonitorEndpoint.start();

      final String hostname = RpcUtils.getHostname(rpcService);

      resourceManagerMetricGroup = ResourceManagerMetricGroup.create(metricRegistry, hostname);
      resourceManager = resourceManagerFactory.createResourceManager(
         configuration,
         ResourceID.generate(),
         rpcService,
         highAvailabilityServices,
         heartbeatServices,
         fatalErrorHandler,
         new ClusterInformation(hostname, blobServer.getPort()),
         webMonitorEndpoint.getRestBaseUrl(),
         resourceManagerMetricGroup);

      final HistoryServerArchivist historyServerArchivist = HistoryServerArchivist.createHistoryServerArchivist(configuration, webMonitorEndpoint);

      final PartialDispatcherServices partialDispatcherServices = new PartialDispatcherServices(
         configuration,
         highAvailabilityServices,
         resourceManagerGatewayRetriever,
         blobServer,
         heartbeatServices,
         () -> MetricUtils.instantiateJobManagerMetricGroup(metricRegistry, hostname),
         archivedExecutionGraphStore,
         fatalErrorHandler,
         historyServerArchivist,
         metricRegistry.getMetricQueryServiceGatewayRpcAddress());

      log.debug("Starting Dispatcher.");
      dispatcherRunner = dispatcherRunnerFactory.createDispatcherRunner(
         highAvailabilityServices.getDispatcherLeaderElectionService(),
         fatalErrorHandler,
         new HaServicesJobGraphStoreFactory(highAvailabilityServices),
         ioExecutor,
         rpcService,
         partialDispatcherServices);

      log.debug("Starting ResourceManager.");
      resourceManager.start();

      resourceManagerRetrievalService.start(resourceManagerGatewayRetriever);
      dispatcherLeaderRetrievalService.start(dispatcherGatewayRetriever);

      return new DispatcherResourceManagerComponent(
         dispatcherRunner,
         resourceManager,
         dispatcherLeaderRetrievalService,
         resourceManagerRetrievalService,
         webMonitorEndpoint);
}
```

Starting Dispatcher REST endpoint，Dispatcher REST为WebMonitorEndpoint类继承自RestServerEndpoint

```
public class WebMonitorEndpoint<T extends RestfulGateway> extends RestServerEndpoint implements LeaderContender, JsonArchivist
```

Starting Dispatcher，创建DispatcherRunner，由LeaderElectionService启动DispatcherRunner

```
public DispatcherRunner createDispatcherRunner(
      LeaderElectionService leaderElectionService,
      FatalErrorHandler fatalErrorHandler,
      JobGraphStoreFactory jobGraphStoreFactory,
      Executor ioExecutor,
      RpcService rpcService,
      PartialDispatcherServices partialDispatcherServices) throws Exception {

   final DispatcherLeaderProcessFactory dispatcherLeaderProcessFactory = dispatcherLeaderProcessFactoryFactory.createFactory(
      jobGraphStoreFactory,
      ioExecutor,
      rpcService,
      partialDispatcherServices,
      fatalErrorHandler);

   return DefaultDispatcherRunner.create(
      leaderElectionService,
      fatalErrorHandler,
      dispatcherLeaderProcessFactory);
}



public static DispatcherRunner create(
			LeaderElectionService leaderElectionService,
			FatalErrorHandler fatalErrorHandler,
			DispatcherLeaderProcessFactory dispatcherLeaderProcessFactory) throws Exception {
		final DefaultDispatcherRunner dispatcherRunner = new DefaultDispatcherRunner(
			leaderElectionService,
			fatalErrorHandler,
			dispatcherLeaderProcessFactory);
		return DispatcherRunnerLeaderElectionLifecycleManager.createFor(dispatcherRunner, leaderElectionService);
	}
	
	

private DispatcherRunnerLeaderElectionLifecycleManager(T dispatcherRunner, LeaderElectionService leaderElectionService) throws Exception {
		this.dispatcherRunner = dispatcherRunner;
		this.leaderElectionService = leaderElectionService;

		leaderElectionService.start(dispatcherRunner);
	}
```

Starting ResourceManager，ResourceManager继承自FencedRpcEndpoint即RpcEndpoint

```
public abstract class ResourceManager<WorkerType extends ResourceIDRetrievable>
      extends FencedRpcEndpoint<ResourceManagerId>
      implements ResourceManagerGateway, LeaderContender
```

创建所有组件成功，返回DispatcherResourceManagerComponent类

```
DispatcherResourceManagerComponent(
      @Nonnull DispatcherRunner dispatcherRunner,
      @Nonnull ResourceManager<?> resourceManager,
      @Nonnull LeaderRetrievalService dispatcherLeaderRetrievalService,
      @Nonnull LeaderRetrievalService resourceManagerRetrievalService,
      @Nonnull WebMonitorEndpoint<?> webMonitorEndpoint) {
   this.dispatcherRunner = dispatcherRunner;
   this.resourceManager = resourceManager;
   this.dispatcherLeaderRetrievalService = dispatcherLeaderRetrievalService;
   this.resourceManagerRetrievalService = resourceManagerRetrievalService;
   this.webMonitorEndpoint = webMonitorEndpoint;
   this.terminationFuture = new CompletableFuture<>();
   this.shutDownFuture = new CompletableFuture<>();

   registerShutDownFuture();
}
```