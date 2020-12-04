flink中的TaskManager，Jobmanager，Dispatcher及ResourceManager之间的通信均使用Akka，以TaskManager为例看下Akka的实现。

MiniCluster启动TaskExecutor：

```
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

这里会创建RpcService，taskManagerRpcServiceFactory.createRpcService()，`RpcService` 是 `RpcEndpoint` 的运行时环境， `RpcService` 提供了启动 `RpcEndpoint`, 连接到远端 `RpcEndpoint` 并返回远端 `RpcEndpoint` 的代理对象等方法。此外， `RpcService` 还提供了某些异步任务或者周期性调度任务的方法。

根据组件是否部署在单一机器上分为CommonRpcServiceFactory和DedicatedRpcServiceFactory，看下分布式部署的DedicatedRpcServiceFactory

```
protected class DedicatedRpcServiceFactory implements RpcServiceFactory {

   private final AkkaRpcServiceConfiguration akkaRpcServiceConfig;
   private final String jobManagerBindAddress;

   DedicatedRpcServiceFactory(AkkaRpcServiceConfiguration akkaRpcServiceConfig, String jobManagerBindAddress) {
      this.akkaRpcServiceConfig = akkaRpcServiceConfig;
      this.jobManagerBindAddress = jobManagerBindAddress;
   }

   @Override
   public RpcService createRpcService() {
      final RpcService rpcService = MiniCluster.this.createRpcService(akkaRpcServiceConfig, true, jobManagerBindAddress);

      synchronized (lock) {
         rpcServices.add(rpcService);
      }

      return rpcService;
   }
}


protected RpcService createRpcService(
			AkkaRpcServiceConfiguration akkaRpcServiceConfig,
			boolean remoteEnabled,
			String bindAddress) {

		final Config akkaConfig;

		if (remoteEnabled) {
			akkaConfig = AkkaUtils.getAkkaConfig(akkaRpcServiceConfig.getConfiguration(), bindAddress, 0);
		} else {
			akkaConfig = AkkaUtils.getAkkaConfig(akkaRpcServiceConfig.getConfiguration());
		}

		final Config effectiveAkkaConfig = AkkaUtils.testDispatcherConfig().withFallback(akkaConfig);

//创建ActorSystem
		final ActorSystem actorSystem = AkkaUtils.createActorSystem(effectiveAkkaConfig);

		return new AkkaRpcService(actorSystem, akkaRpcServiceConfig);
	}
```

AkkaRpcService实现RpcService接口，是线程安全类

```
@ThreadSafe
public class AkkaRpcService implements RpcService {

   private static final Logger LOG = LoggerFactory.getLogger(AkkaRpcService.class);

   static final int VERSION = 1;

   private final Object lock = new Object();

   private final ActorSystem actorSystem;
   private final AkkaRpcServiceConfiguration configuration;

   @GuardedBy("lock")
   private final Map<ActorRef, RpcEndpoint> actors = new HashMap<>(4);

   private final String address;
   private final int port;

   private final ScheduledExecutor internalScheduledExecutor;

   private final CompletableFuture<Void> terminationFuture;

   private volatile boolean stopped;
```

将RpcService传入TaskExecutor的构造函数，super调用RpcEndpoint的构造函数

```
public TaskExecutor(
      RpcService rpcService,
      TaskManagerConfiguration taskManagerConfiguration,
      HighAvailabilityServices haServices,
      TaskManagerServices taskExecutorServices,
      HeartbeatServices heartbeatServices,
      TaskManagerMetricGroup taskManagerMetricGroup,
      String metricQueryServiceAddress,
      BlobCacheService blobCacheService,
      FatalErrorHandler fatalErrorHandler,
      TaskExecutorPartitionTracker partitionTracker,
      BackPressureSampleService backPressureSampleService) {

   super(rpcService, AkkaRpcServiceUtils.createRandomName(TASK_MANAGER_NAME));
```

TaskExecutor实现了RpcEndpoint和TaskExecutorGateway，TaskExecutorGateway实现了RpcGateway，是RpcEndpoint的代理对象。

`RpcEndpoint` 是对 RPC 框架中提供具体服务的实体的抽象，所有提供远程调用方法的组件都需要继承该抽象类。另外，对于同一个 `RpcEndpoint` 的所有 RPC 调用都会在同一个线程（RpcEndpoint 的“主线程”）中执行，因此无需担心并发执行的线程安全问题。

`RpcGateway` 接口是用于远程调用的代理接口。 `RpcGateway` 提供了获取其所代理的 `RpcEndpoint` 的地址的方法。在实现一个提供 RPC 调用的组件时，通常需要先定一个接口，该接口继承 `RpcGateway` 并约定好提供的远程调用的方法。

```
public class TaskExecutor extends RpcEndpoint implements TaskExecutorGateway
```

RpcEndpoint的构造函数，由AkkaRpcService启动rpcServer

`RpcServer` 相当于 `RpcEndpoint` 自身的的代理对象（self gateway)。`RpcServer` 是 `RpcService` 在启动了 `RpcEndpoint` 之后返回的对象，每一个 `RpcEndpoint` 对象内部都有一个 `RpcServer` 的成员变量，通过 `getSelfGateway` 方法就可以获得自身的代理，然后调用该Endpoint 提供的服务。

```
protected RpcEndpoint(final RpcService rpcService, final String endpointId) {
   this.rpcService = checkNotNull(rpcService, "rpcService");
   this.endpointId = checkNotNull(endpointId, "endpointId");

   this.rpcServer = rpcService.startServer(this);

   this.mainThreadExecutor = new MainThreadExecutor(rpcServer, this::validateRunsInMainThread);
}
```

AkkaRpcService的startServer，主要工作包括： - 创建一个 Akka actor （`AkkaRpcActor` 或 `FencedAkkaRpcActor`） - 通过动态代理创建代理对象

```
public <C extends RpcEndpoint & RpcGateway> RpcServer startServer(C rpcEndpoint) {
   checkNotNull(rpcEndpoint, "rpc endpoint");

   CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
   final Props akkaRpcActorProps;

   if (rpcEndpoint instanceof FencedRpcEndpoint) {
      akkaRpcActorProps = Props.create(
         FencedAkkaRpcActor.class,
         rpcEndpoint,
         terminationFuture,
         getVersion(),
         configuration.getMaximumFramesize());
   } else {
      akkaRpcActorProps = Props.create(
         AkkaRpcActor.class,
         rpcEndpoint,
         terminationFuture,
         getVersion(),
         configuration.getMaximumFramesize());
   }

   ActorRef actorRef;

// 创建 Akka actor，与rpcEndpoint对应
   synchronized (lock) {
      checkState(!stopped, "RpcService is stopped");
      actorRef = actorSystem.actorOf(akkaRpcActorProps, rpcEndpoint.getEndpointId());
      actors.put(actorRef, rpcEndpoint);
   }

   LOG.info("Starting RPC endpoint for {} at {} .", rpcEndpoint.getClass().getName(), actorRef.path());

   final String akkaAddress = AkkaUtils.getAkkaURL(actorSystem, actorRef);
   final String hostname;
   Option<String> host = actorRef.path().address().host();
   if (host.isEmpty()) {
      hostname = "localhost";
   } else {
      hostname = host.get();
   }

// 代理的接口
   Set<Class<?>> implementedRpcGateways = new HashSet<>(RpcUtils.extractImplementedRpcGateways(rpcEndpoint.getClass()));

   implementedRpcGateways.add(RpcServer.class);
   implementedRpcGateways.add(AkkaBasedEndpoint.class);

//创建 InvocationHandler
   final InvocationHandler akkaInvocationHandler;

   if (rpcEndpoint instanceof FencedRpcEndpoint) {
      // a FencedRpcEndpoint needs a FencedAkkaInvocationHandler
      akkaInvocationHandler = new FencedAkkaInvocationHandler<>(
         akkaAddress,
         hostname,
         actorRef,
         configuration.getTimeout(),
         configuration.getMaximumFramesize(),
         terminationFuture,
         ((FencedRpcEndpoint<?>) rpcEndpoint)::getFencingToken);

      implementedRpcGateways.add(FencedMainThreadExecutable.class);
   } else {
      akkaInvocationHandler = new AkkaInvocationHandler(
         akkaAddress,
         hostname,
         actorRef,
         configuration.getTimeout(),
         configuration.getMaximumFramesize(),
         terminationFuture);
   }
   // Rather than using the System ClassLoader directly, we derive the ClassLoader
		// from this class . That works better in cases where Flink runs embedded and all Flink
		// code is loaded dynamically (for example from an OSGI bundle) through a custom ClassLoader
		ClassLoader classLoader = getClass().getClassLoader();

//通过动态代理创建代理对象
		@SuppressWarnings("unchecked")
		RpcServer server = (RpcServer) Proxy.newProxyInstance(
			classLoader,
			implementedRpcGateways.toArray(new Class<?>[implementedRpcGateways.size()]),
			akkaInvocationHandler);

		return server;
	}
```

ActorRef是akka包提供的类，是scala代码

```
package akka.actor
abstract class ActorRef() extends scala.AnyRef with java.lang.Comparable[akka.actor.ActorRef] with scala.Serializable {
 this : akka.actor.InternalActorRef =>
  def path : akka.actor.ActorPath
  final def compareTo(other : akka.actor.ActorRef) : scala.Int = { /* compiled code */ }
  final def tell(msg : scala.Any, sender : akka.actor.ActorRef) : scala.Unit = { /* compiled code */ }
  def forward(message : scala.Any)(implicit context : akka.actor.ActorContext) : scala.Unit = { /* compiled code */ }
  @scala.deprecated("Use context.watch(actor) and receive Terminated(actor)", "2.2")
  private[akka] def isTerminated : scala.Boolean
  final override def hashCode() : scala.Int = { /* compiled code */ }
  final override def equals(that : scala.Any) : scala.Boolean = { /* compiled code */ }
  override def toString() : scala.Predef.String = { /* compiled code */ }
}
object ActorRef extends scala.AnyRef with scala.Serializable {
  final val noSender : akka.actor.ActorRef = { /* compiled code */ }
}
```

创建成功RpcServer之后，在taskExecutor.start()的时候会启动RpcServer

```
public final void start() {
   rpcServer.start();
}
```

AkkaInvocationHandler 实现 rpcServer，最终由AkkaInvocationHandler 启动rpcServer

`RpcServer` 是通过 `AkkaInvocationHandler` 创建的动态代理对象，所以启动 `RpcEndpoint` 实际上就是向当前 endpoint 绑定的 Actor 发送一条 START 消息，通知服务启动。

```
public void start() {
   rpcEndpoint.tell(ControlMessages.START, ActorRef.noSender());
}
```

RpcEndpoint的getSelfGateway方法可以直接获取本地代理，即rpcServer，一般供测试使用

```
public <C extends RpcGateway> C getSelfGateway(Class<C> selfGatewayType) {
   if (selfGatewayType.isInstance(rpcServer)) {
      @SuppressWarnings("unchecked")
      C selfGateway = ((C) rpcServer);

      return selfGateway;
   } else {
      throw new RuntimeException("RpcEndpoint does not implement the RpcGateway interface of type " + selfGatewayType + '.');
   }
}
```

而一般的rpc通信都需要获取远程代理，例如ResourceManager通过SlotManager给TaskExecutor分配Slot，需要从TaskExecutorConnection拿到TaskExecutorGateway远程代理。

```
private void allocateSlot(TaskManagerSlot taskManagerSlot, PendingSlotRequest pendingSlotRequest) {
   Preconditions.checkState(taskManagerSlot.getState() == TaskManagerSlot.State.FREE);

   TaskExecutorConnection taskExecutorConnection = taskManagerSlot.getTaskManagerConnection();
   TaskExecutorGateway gateway = taskExecutorConnection.getTaskExecutorGateway();
```