### flink-connector-elasticsearch介绍

在实际应用中，一些明细数据需要存入Elasticsearch来供使用方快速的查询。而flink内部集成了Elasticsearch的connector可以直接使用封装的api。这里通过示例记录一下flink任务写入es的sink的方式。

![](2.png)

可以看到flink支持es的各种版本，从2.x到7.x。然后高版本是可以兼容低版本的写入的。代码方面主要分成了5个module: flink-connector-elasticsearch2，flink-connector-elasticsearch5，flink-connector-elasticsearch6，flink-connector-elasticsearch7，flink-connector-elasticsearch-base。其中flink-connector-elasticsearch-base定义了通用的接口，其他module为不同版本的实现。

![](3.png)

上图是主要的代码结构，flink-connector-elasticsearch7与flink-connector-elasticsearch6的结构基本一致。可以看到flink-connector-elasticsearch6之后做了很多调整，封装也更加完善。下面主要看一下

flink-connector-elasticsearch的使用。

这里演示两种方式，一种是使用ElasticsearchSink API，这种方式需要自己实现ElasticsearchSinkFunction接口进行增删改。另一种方式是使用flink-connector-elasticsearch自己封装的ElasticsearchUpsertTableSink，这种使用方式比较简单，直接传入es集群的相关参数就可以了。传参的方式一种是使用 descriptor API，也就是 org.apache.flink.table.descriptors.Elasticsearch，通过tableApi进行connect。或者通过DDL直接定义。

### ElasticsearchSink 

先看下ElasticsearchSink 的实现，分别比较一下5.x和6.x和7.x。

#### ElasticsearchSink 5.x

```
Properties properties = new Properties();
properties.setProperty("bootstrap.servers", KAFKA_SERVERS);
properties.setProperty("group.id", GROUP_ID);
FlinkKafkaConsumer011<String> depponConsumer = new FlinkKafkaConsumer011<>(KAFKA_TOPIC, new ExchangeDeserializer(), properties);

DataStream<String> dataStream = env.addSource(depponConsumer)
DataStream<FmsNetTob> smsNetTobStream = dataStream
        .map(new DataMapFunction())
        .addSink(buildEalsticSink());
```

从kafka读取消息并进行清洗，然后写入es。看下buildEalsticSink方法

```
ElasticsearchSink<FmsNetTob> buildEalsticSink() {
    Map<String, String> userConfig = ElasticsearchConfigHelper.loadYmalConfig("elasticsearch.yml");
    // ES客户端封装
    ElasticsearchApiCallBridge<TransportClient> bridge = new Elasticsearch5ApiCallBridge(ElasticsearchConfigHelper.getCluster(Constants.ELASTIC_SERVERS));
     
    //重试策略
    ActionRequestFailureHandler failureHandler = new NoOpFailureHandler();
    if(Constants.ELASTIC_FAILHANDLER) {
        failureHandler = new RetryRejectedExecutionFailureHandler();
    }
    //es
    ElasticsearchSinkFunction<FmsNetTob> elasticSinkFunction = new DepponSinkFunction(Constants.INDEX_NAME, Constants.INDEX_TYPE);
    
    ElasticsearchSink<FmsNetTob> elasticSink = new ElasticsearchSink<FmsNetTob>(bridge, userConfig, elasticSinkFunction, failureHandler);
    
    return elasticSink;
}
```

ElasticsearchSink需要自定义Elasticsearch5ApiCallBridge，ElasticsearchSinkFunction，ActionRequestFailureHandler等类并传入ElasticsearchSink的构造函数。看下ElasticsearchSinkFunction的实现。

```
public class DepponSinkFunction implements ElasticsearchSinkFunction<FmsNetTob> {
    //索引名
    private String index;

    //type名
    private String type;

    public DepponSinkFunction(String index, String type) {
        this.index = index;
        this.type = type;
    }

    @Override
    public void process(FmsNetTob source, RuntimeContext context, RequestIndexer indexer) {
        indexer.add(indexRequest(source));
    }

    public IndexRequest indexRequest(FmsNetTob source) {
        JSONObject jsonSource = (JSONObject) JSON.toJSON(source);
        IndexRequest indexRequest = Requests.indexRequest().index(index).type(type).source(jsonSource.toJSONString(), XContentType.JSON);
        return indexRequest;
    }
}
```

实现ElasticsearchSinkFunction接口的process方法实现es写入，es以id去重，插入时不指定id会随机生成一个id。插入结果如下：

![](1.png)

#### ElasticsearchSink 6.x

使用相同的pipeline流程：

```
Properties properties = new Properties();
properties.setProperty("bootstrap.servers", KAFKA_SERVERS);
properties.setProperty("group.id", GROUP_ID);
FlinkKafkaConsumer011<String> depponConsumer = new FlinkKafkaConsumer011<>(KAFKA_TOPIC, new ExchangeDeserializer(), properties);

DataStream<String> dataStream = env.addSource(depponConsumer)
DataStream<FmsNetTob> smsNetTobStream = dataStream
        .map(new DataMapFunction())
        .addSink(buildEalsticSink());
```

区别主要在buildEalsticSink过程：

```
ElasticsearchSink<FmsNetTob> buildEalsticSink() {
    List<HttpHost> httpHosts = new ArrayList<>();
    httpHosts.add(new HttpHost("10.xxx.xxx.xxx", 9200, "http"));
    httpHosts.add(new HttpHost("10.xxx.xxx.xxx", 9200, "http"));
    httpHosts.add(new HttpHost("10.xxx.xxx.xxx", 9200, "http"));

    ElasticsearchSink.Builder<FmsNetTob> esSinkBuilder = new ElasticsearchSink.Builder<FmsNetTob>(
            httpHosts, new DepponEsfunction("fms_net_tob", "fms_net_tob_balance"));

    esSinkBuilder.setBulkFlushMaxActions(Constants.ELASTIC_BULK_FLUSH_MAX_ACTIONS);
    esSinkBuilder.setBulkFlushInterval(Constants.ELASTIC_BULK_FLUSH_INTERVAL_MS);
    esSinkBuilder.setBulkFlushMaxSizeMb(Constants.ELASTIC_BULK_FLUSH_MAX_SIZE_MB);

    ActionRequestFailureHandler failureHandler;
    if(Constants.ELASTIC_BULK_FLUSH_BACKOFF_ENABLE) {
        failureHandler = new RetryRejectedExecutionFailureHandler();
        esSinkBuilder.setBulkFlushBackoff(true);
        esSinkBuilder.setFailureHandler(failureHandler);
        esSinkBuilder.setBulkFlushBackoffDelay(Constants.ELASTIC_BULK_FLUSH_BACKOFF_DELAY);
        esSinkBuilder.setBulkFlushBackoffRetries(Constants.ELASTIC_BULK_FLUSH_BACKOFF_RETRIES);
        esSinkBuilder.setBulkFlushBackoffType(ElasticsearchSinkBase.FlushBackoffType.CONSTANT);
    }

    return esSinkBuilder.build();
}
```

ElasticsearchSink 6.x增加了Builder构造器，并且将Elasticsearch6ApiCallBridge批处理封装进了ElasticsearchSink内部。构造时只需添加set进需要的参数就可以。

```
private ElasticsearchSink(
   Map<String, String> bulkRequestsConfig,
   List<HttpHost> httpHosts,
   ElasticsearchSinkFunction<T> elasticsearchSinkFunction,
   ActionRequestFailureHandler failureHandler,
   RestClientFactory restClientFactory) {

//Elasticsearch6ApiCallBridge在初始化ElasticsearchSink时被内部初始化
   super(new Elasticsearch6ApiCallBridge在初始化(httpHosts, restClientFactory),  bulkRequestsConfig, elasticsearchSinkFunction, failureHandler);
}
```

ElasticsearchSinkBase类的一些初始化和invoke方法，ElasticsearchSinkBase实现了RichSinkFunction和CheckpointedFunction

```
@Override
public void open(Configuration parameters) throws Exception {
   client = callBridge.createClient(userConfig);
   //构造批处理类
   bulkProcessor = buildBulkProcessor(new BulkProcessorListener());
   requestIndexer = callBridge.createBulkProcessorIndexer(bulkProcessor, flushOnCheckpoint, numPendingRequests);
   failureRequestIndexer = new BufferingNoOpRequestIndexer();
}

@Override
public void invoke(T value, Context context) throws Exception {
   checkAsyncErrorsAndRequests();
   //调用elasticsearchSinkFunction的具体process
   elasticsearchSinkFunction.process(value, getRuntimeContext(), requestIndexer);
}

@Override
public void initializeState(FunctionInitializationContext context) throws Exception {
   // no initialization needed
}

@Override
public void snapshotState(FunctionSnapshotContext context) throws Exception {
   checkAsyncErrorsAndRequests();
//checkpoint的时候把批处理的数据刷进es
   if (flushOnCheckpoint) {
      while (numPendingRequests.get() != 0) {
         bulkProcessor.flush();
         checkAsyncErrorsAndRequests();
      }
   }
}
```

ElasticsearchSinkFunction的实现与5.x基本一致。不再重述。

#### ElasticsearchSink 7.x

ElasticsearchSink 7.x与6.x的使用是基本一致的，唯一的区别是ElasticsearchSink 7.x废弃了插入时的type参数，直接指定id进行插入和更新。

### descriptor API

ElasticsearchSink 6.x之后提供了ElasticsearchUpsertTableSink的封装，可以传入参数就可以实现es的插入和更新，无需再实现ElasticsearchSinkFunction。descriptor API的用法是通过tableApi来connect org.apache.flink.table.descriptors.Elasticsearch类，如下是一个示例，这里使用了flink的1.10的版本，废弃了registerTableSink\Source改用createTemporaryTable，废弃了deriveScheme方法等,zookeeper.connect也为必填参数

```
public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    Schema schema = new Schema()
            .field("busyId", DataTypes.STRING())
            .field("account", DataTypes.STRING())
            .field("userMobile", DataTypes.STRING())
            .field("expressCompanyName", DataTypes.STRING())
            .field("expressCompanyCode", DataTypes.STRING())
            .field("busyType", DataTypes.INT())
            .field("gainMoney", DataTypes.DOUBLE())
            .field("netInbillMoney", DataTypes.DOUBLE())
            .field("inbillMoney", DataTypes.DOUBLE())
            .field("payType", DataTypes.STRING())
            .field("payTm", DataTypes.BIGINT())
            .field("paySerialNumber", DataTypes.STRING())
            .field("remark", DataTypes.STRING())
            .field("userUicId", DataTypes.BIGINT())
            .field("province", DataTypes.STRING())
            .field("city", DataTypes.STRING())
            .field("district", DataTypes.STRING())
            .field("postId", DataTypes.STRING())
            .field("edCode", DataTypes.STRING())
            .field("throwAddress", DataTypes.STRING())
            .field("expressId", DataTypes.STRING())
            .field("boxType", DataTypes.INT())
            .field("isRefund", DataTypes.INT())
            .field("serviceName", DataTypes.STRING());

    tableEnv.connect(
            new Kafka()
                    .topic("testtest1")
                    .property("bootstrap.servers", "10.xxx.xxx.xxx:9092,10.xxx.xxx.xxx:9092,10.xxx.xxx.xxx:9092")
                    .property("group.id", "test")
                    .property("zookeeper.connect","10.xxx.xxx.xxx:2181,10.xxx.xxx.xxx:2181")
                    .startFromLatest()
                    .version("0.11")
            )
            .withSchema(schema)
            .withFormat(new Json())
            .createTemporaryTable("Users");

    Table table = tableEnv.from("Users");

    // print
    tableEnv.toAppendStream(table, TypeInformation.of(Row.class)).print();

    tableEnv.connect(
            new Elasticsearch()
                    .version("6")
                    .host("10.xxx.xxx.xxx", 9200, "http")
                    .index("fms_net_tob")
                    //文档
                    .documentType("fms_net_tob_balance")
                    // key之间的分隔符，默认"#"
                    .keyDelimiter("#")
                    // 如果key为null，则用""字符串替代
                    .keyNullLiteral("")
                    // 失败处理策略，Fail（报错，job失败），Ignore(失败)，RetryRejected（重试），Custom（自己定义）
                    .failureHandlerIgnore()
                    // 关闭flush检测
                    .disableFlushOnCheckpoint()
                    // 为每个批量请求设置要缓冲的最大操作数
                    .bulkFlushMaxActions(5)
                    // 每个批量请求的缓冲最大值，目前仅支持 MB
                    .bulkFlushMaxSize("5 MB")
                    // 每个批量请求间隔时间
                    .bulkFlushInterval(1000L)
                    // 设置刷新批量请求时要使用的常量回退类型
                    .bulkFlushBackoffConstant()
                    // 设置刷新批量请求时每次回退尝试之间的延迟量（毫秒）
                    .bulkFlushBackoffDelay(30000L)
                    // 设置刷新批量请求时回退尝试的最大重试次数。
                    .bulkFlushBackoffMaxRetries(3)
            )
            .withSchema(schema)
            .withFormat(new Json())
            //可以选择是append还是upsert
            .inUpsertMode()
            .createTemporaryTable("elsearch");

    table.insertInto("elsearch");
    env.execute("SqlSinkElasticSearchStream");
}
}
```

已上便实现了已table的形式插入es。DDL的方式类似，将Elasticsearch参数设置的过程转移到CREATE TABLE 的WITH参数下。运行程序可以看到插入的效果。

![](1.png)

![](4.png)

据官方文档描述sql语句可以通过 SELECT a, b, c FROM t GROUP BY a, b 的方式指定a和b为id，即a#b。

修改sqlQuery为下面这样，则自动根据payTm,paySerialNumber的顺序生成id：

```
Table table = tableEnv.sqlQuery("select sum(busyType) as busyType, payTm,paySerialNumber from Users GROUP BY payTm,paySerialNumber");
```

发送消息如下：

> {"paySerialNumber":"PFC0105434202006021516032687707","payTm":1591082163641,"busyType":2}

则插入结果如下：

![](5.png)

id即为payTm#paySerialNumber

再次发送消息，busyType被更新成了4：

![](6.png)

