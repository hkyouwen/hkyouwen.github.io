### kafka Schema Registry介绍

Confluent Schema Registry为你的元数据提供一个服务层接口，用来存储和查询Apache Avro schema。它根据指定的主题(subject)命名策略存储所有schema的版本历史记录，提供多种兼容性设置，并允许根据配置的兼容性设置(compatibility settings)和扩展的Avro支持来演进架构。它提供了可插拔的Kafka客户端的序列化程序，处理以Avro格式发送的Kafka消息schema的存储和检索。


Schema Registry位于Kafka broker之外。生产者和消费者仍在与Kafka通信，以发布和读取topic的消息。但同时他们还可以与Schema Registry进行通信，以发送和检索描述消息数据模型的schema。

![](1.png)

Schema Registry是Avro schema的分布式存储层，该架构使用Kafka作为其底层存储机制，其中一些关键设计：
1，为每个注册的schema分配全局唯一ID。保证分配的ID单调增加，但不一定连续。
2，Kafka提供持久的存储，并为Schema Registry的状态信息及其包含的schema提供WAL。
3，Schema Registry设计为具有单一主节点的分布式架构，使用ZooKeeper / Kafka进行主节点选举（基于配置）。

### 几个概念区分

**topic** ：Kafka Topic包含消息，并且每个消息都是key-value键值对。消息key和value两者可以同时或单独序列化为Avro格式。
**schema** ： schema定义了Avro数据格式的结构，Kafka topic命名可以独立于schema命名。
**subject** ： schema注册表定义了可以在其中演化的schema范围，该范围就是subject。subject名称取决于配置的subject名称策略，默认情况下，该策略设置为从topic名称派生subject名称。

举例：
有一个topic名为testtest3，将该topic的value值进行avro序列化，则默认配置的subject名为"testtest3-value"。  schema命名与topic独立，例如注册一个名为Payment的shema：
```
{
"type": "record", 
"name": "Payment", 
"fields": [ {"name": "id", "type": "string"}, {"name": "amount", "type": "double"} ]
}
```
可在Schema Registry中查询到如下信息
![](2.png)

### Avro背景

通过网络发送数据或将其存储在文件中时，我们需要一种将数据编码为字节的方法。起初是使用从编程语言特定的序列化，例如Java序列化，这使得使用其他语言的数据使用起来很不方便。然后开始使用与语言无关的格式，例如JSON。但是，JSON之类的格式缺少严格定义的格式，这有两个明显的缺点：
1，数据使用者可能无法理解数据产生者：缺乏结构会使使用这些格式的数据更具挑战性，因为可以随意添加或删除字段，甚至可能损坏数据。随着组织中越来越多的应用程序或团队开始使用数据源，此缺陷将变得更加严重：如果上游团队可以自行决定对数据格式进行任意更改，那么要确保所有下游消费者能够解析数据。缺少的是生产者和消费者之间的数据约束，类似于API。
2，开销和冗长：它们很冗长，因为字段名称和类型信息必须以序列化格式显式表示，尽管它们在所有消息中都相同。
已经出现了一些跨语言序列化库，这些库要求通过某种模式来正式定义数据结构。这些库包括Avro， Thrift和
Protocol Buffers。具有模式的优点是它可以清楚地指定数据的结构，类型和含义。使用模式，还可以更有效地对数据进行编码。我们推荐Confluent Platform支持的Avro。Avro模式以JSON格式定义数据结构。以下是一个示例Avro模式，该模式指定了一个用户记录，其中包含两个字段：name和favorite_number 类型分别为string和int。

```
{"namespace": "example.avro",
 "type": "record",
 "name": "user",
 "fields": [
     {"name": "name", "type": "string"},
     {"name": "favorite_number",  "type": "int"}
 ]}
```

然后可以使用该Avro模式，例如，将Java对象（POJO）序列化为字节，然后将这些字节反序列化为Java对象。
关于Avro的有趣的事情之一是，它不仅在数据序列化期间需要schema，而且在数据反序列化期间也需要schema。由于schema是在解码时提供的，因此不必在数据中明确编码诸如字段名之类的元数据。这使得Avro数据的二进制编码非常紧凑。

### Schema ID分配

Schema ID分配始终在主节点中发生，并且Schema ID总是单调增加。如果您使用的是Kafka主选举，则Schema ID始终基于写入Kafka的最后一个ID。在主节点重选期间，仅在新的主节点赶上了存储<kafkastore.topic>中的所有记录之后才进行Schema ID批次分配。
如果使用的是ZooKeeper的选举，/<schema.registry.zk.namespace>/schema_id_counter 路径存储当前schema ID批次范围的上届，新的ID批次分配由当前的批次范围和主节点选举共同触发。按批次的分配有助于防止潜在的僵尸主节点（例如，如果之前的主节点进行GC暂停持续的时间大于ZooKeeper超时事件，则触发主节点的重选）。

### kafka存储后端

Kafka用作Schema Registry存储后端。具有单个分区的特殊Kafka topic<kafkastore.topic>（默认_schemas）用作高可用性的预写日志。所有Schema，subject/version和ID元数据以及兼容性设置都作为消息附加到此日志中。因此，Schema Registry实例在该_schemas topic下既生产又消费消息。例如，当在subject下注册新schema时，或者在注册对兼容性设置的更新时，它将向日志生产消息。Schema Registry用后台线程从_schemas中消费，并在消费_schemas消息时更新其本地缓存，以反映新添加的Schema或兼容性设置。以这种方式从Kafka日志更新本地状态可确保持久性，有序性和易于恢复性。

Schema Registry topic是压缩的，因此无论Kafka保留策略如何，每个key的最新值都将被永久保留。您可以使用以下方法进行验证kafka-configs：
`$ kafka-configs --zookeeper localhost:2181 --entity-type topics --entity-name _schemas --describe
Configs for topic '_schemas' are cleanup.policy=compact`

### 单一主节点架构

Schema Registry设计为使用单个主节点架构的分布式服务。在这种配置下，在任何时刻最多有一个Schema Registry实例是主实例。只有主节点才能够将写操作发布到底层的Kafka日志，但是所有节点都能够直接提供读取请求服务。从节点通过简单地将注册请求转发到当前主节点并返回主节点提供的响应来间接为注册请求提供服务。在Schema Registry版本4.0之前，主节点选举始终通过ZooKeeper进行协调。现在，也可以选择通过Kafka协议进行选举。

kafka选举：

![](4.png)


如果<kafkastore.connection.url>未配置并且<kafkastore.bootstrap.servers>指定了Kafka broker，则会选择基于Kafka的主选举。kafka group协议从所有节点中选择一个作为主节点master.eligibility=true。在没有ZooKeeper的情况下，例如在托管或云Kafka环境中，或者对ZooKeeper的访问已被锁定的情况下，可以使用基于Kafka的选举。


ZooKeeper 选举：

![](5.png)


当在配置中指定ZooKeeper URL<kafkastore.connection.url>时，选择ZooKeeper主选举。当前的主节点在在ZooKeeper的/<schema.registry.zk.namespace>/schema_registry_master路径中的临时节点中维护。Schema Registry节点侦听此路径上的数据更改和删除事件，并且主进程的关闭或失败会触发每个节点设置master.eligibility=true并参与新的一轮选举。主选举是一个简单的“先写入获胜”策略：成功将其自己的数据写入/<schema.registry.zk.namespace>/schema_registry_master的第一个节点是新的主数据库。