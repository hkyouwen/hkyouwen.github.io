最近遇到了一个使用了flink的[Side Outputs](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/side_output.html)的场景，这里记录一下。

需求的场景比较简单，从kafka接收用户的微信id去实时查询微信的接口获取用户的信息并写入数据库。接口是批量查询接口，可以一次传入多个id。因此有一个问题，如果一批id中有一个非法id，会导致同一批数据全部返回失败。这就需要把返回结果为失败的这一批id全部提取出来并且重新拆分进行查询。

这就需要用到分流，把查询成功的流输出到数据库，把查询失败的流输出到一个地方保存并重新解析。在flink中sideout(侧输出)就是解决这种问题的工具。

sideoutput的输出类型可以与主流不同，可以有多个sideoutput，每个侧输出不同的类型。在使用侧输出的时候需要先定义一个OutputTag。定义方式，如下：

```
OutputTag<String> outputTag = newOutputTag<String>("side-output") {};
```

OutputTag有两个构造函数，只有一个id参数，或者包括两个参数，id以及TypeInformation 的信息。

```
OutputTag(String id)
OutputTag(String id, TypeInformation<T>typeInfo)
```

使用sideoutput需要在特定的函数里，通常是更底层的API如ProcessFunction里面，在这些函数里可以使用Context参数，这个参数在底层的API里是暴露给用户使用的。主要有以下几个API：

- [ProcessFunction](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/process_function.html)
- [KeyedProcessFunction](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/process_function.html#the-keyedprocessfunction)
- CoProcessFunction
- KeyedCoProcessFunction
- [ProcessWindowFunction](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/windows.html#processwindowfunction)
- ProcessAllWindowFunction

以下是部分代码示例：

1，首先是flink主流程，需要先定义OutputTag的输出类型及id，然后把OutputTag传入具体需要使用分流的MAP函数中，最后直接从MAP函数处理得到的OutputStream中调用getSideOutput方法拿到sideOutPut的流，这里是把sideOutPut的流输出到一个文件中保存。

```
private static void execute(StreamExecutionEnvironment env) {

//从kafka接收用户微信id
        DataStream<String> dataStream =
                KafkaSourceBuilder.buildKafkaStreamWithUid(env, KAFKA_SERVERS, GROUP_ID, KAFKA_TOPIC);

//定义OutputTag
        final OutputTag<List<Map<String, String>>> outputTag = new OutputTag<List<Map<String, String>>>("side-output"){};

        SingleOutputStreamOperator<Tuple2<WeiXinUserDTO,Map<String, String>>> singleOutputStreamOperator = dataStream
                .filter(i -> StringUtils.isNotBlank(i))
                .map(new JsonMapFunction())
                .filter(i -> i != null)
            .windowAll(TimeAndCountWindowAssigner.of(Time.seconds(5),100))
                .aggregate(new ListAggregateFunction())
                //这里把outputTag传入HttpWXProcessFunction函数中
                .process(new HttpWXProcessFunction(outputTag))；

//正常的流输出到数据库
        singleOutputStreamOperator
                .addSink(new WxMysqlSink());

//拿到SideOutput输出流
        DataStream<List<Map<String, String>>> sideOutStream = singleOutputStreamOperator.getSideOutput(outputTag);
        
        //使用SideOutput输出流
        sideOutStream
                .addSink(new SideOutSink("/app/weixin/","wexinId","yyyyMMdd"))
```

2，接下来是ProcessFunction函数的执行过程，通过接口查询用户信息。如果成功则使用out.collect发送至下游，如果失败则通过Context将错误的id输出到sideoutput。即ctx.output(outputTag, value)

```
public class HttpWXProcessFunction extends ProcessFunction<List< String> , WeiXinUserDTO>
{
    @Override
    public void processElement(<List< String>> value, Context ctx, Collector<WeiXinUserDTO> out) throws Exception {
        
        ......
        tempList.put("user_list", value);

       String result = transHttpReqFacade.getHttpPostResult ("https://api.weixin.qq.com/cgi-bin/user/info/batchget?access_token=" + accessToken, JSONObject.toJSONString(tempList));
            
            
       WeiXinUserDTO weiXinUserDTO = JSONObject.parseObject(result, WeiXinUserDTO.class);
                
       if (weiXinUserDTO.getUser_info_list().size() > 0) {
           log.info("微信接口成功,tempList size为:{}", weiXinUserDTO .getUser_info_list().size());
           //成功则发送到下游
           out.collect(new Tuple2(weiXinUserDTO, map));
       } else {
           log.error("微信获取接口的值失败为:{},token值为:{},tempList值为:{}", result, accessToken, JSONObject.toJSONString(tempList));
           //失败则输出到sideoutput
           ctx.output(outputTag, value);
      }
            
}
```

最后在运行过程中可以在指定的SideOutPut的输出路径下找到查询失败的id列表文件，重新处理操作就可以了。

>  [:/app/weixin]$
> wexinId.txt