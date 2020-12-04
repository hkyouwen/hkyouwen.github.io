### DataSketches Theta Sketch简介
#### 1.1DataSketches Theta Sketch概念
DataSketches Theta Sketch提供druid数据聚合，再数据摄入的过程创建存储在Druid sedments 上的Theta sketch objects，Theta sketch objects可以理解为一个Set的数据结构。在查询的过程，sketches 被读取并去重。最终，你得到一个sketch object内的元素去重之后的估计值，同时，你可以使用"postAggregations"对sketch object进行union，intersection和not操作。

要使用DataSketches Theta Sketch，确保安装了扩展配置， druid.extensions.loadList=["druid-datasketches"]

DataSketches Theta Sketch更多介绍，参考：
[https://datasketches.github.io/docs/Theta/ThetaSketchFramework.html](https://datasketches.github.io/docs/Theta/ThetaSketchFramework.html)

#### 1.2 DataSketches Theta Sketch原理

DataSketches 是以Theta Sketch Framework (TSF) 框架为基础的。TSF是一个包含了多种sketching算法，并实现了多流数据之间进行集合运算的数学框架。TSF中包含了几个概念：
1，一个数据元组 (θ,S)，θ代表Theta，取值范围0< θ < 1，表示一个阈值。S代表Sketch，是一个数据集合，里面存储着不重复的哈希值x，x的取值范围也是0<x<1,并且x < θ。也是Theta Sketch命名的由来。
2，一个通用的组合函数ThetaUnion，用来实现一组Theta Sketch的并集，交集和差值操作。
3，一个估值函数，根据Theta Sketch来估算出所有不重复的元素的数量。

TSF的理论基础是KMV算法，先简单介绍一下KMV算法。KMV也就是Kth Minimum Value，字面理解就是用K个最小值来估计全部的元素数量。KMV主要解决去重计算中传统的hash算法随着元素数量增加而存储空间和查询时间不断增大的问题。KMV算法的策略是以存储更少的值（k 个值）为基础，从中可以估计出 N 的大小，而且误差范围固定。

其中k值的大小对应上述TSF的Sketch的存储元素个数的上界，用来更新和淘汰Sketch中的元素，同时也决定了最终估计值的准确度。

![](555.png)

上图是KMV的大致原理，假设最左边的输入流中有10个不重复的数字，首先在输入的过程中通过一个HASH函数将这10个数字的值均匀映射为10个大于0且小于1的值，对应上述TSF中的x值，在本例中为0.008到0.922。然后我们选定k值的大小为3，那么将只保留最小的3个哈希值，第k小的hash值就是0.195。最后运用估值函数计算预期结果，KMV的估值函数为(k-1)/Value(k),本例中即为(3-1)/0.195 = 10.26。准确估计出了假设值。

![](666.png)

更近一步，选定了k值之后，大于k的那些hash值可以不需要存储，这样可以节省空间。这样当写入更小的hash时，将新的hash写入，同时更新及淘汰旧的Value(k)即可。需要说明的是如果插入的元素数量达不到k值，则估值就是已有的元素的数量。比如k=3，但
Sketch中只有两个hash，则估值结果就是2。

TSF在KMV的基础上有些小的改动，增加了一个θ的值用来代替Value(k)，θ保存第k+1小的hash值，如果hash数量小于k，则θ值永远为1.0。 两外TSF将KMV中的分子(k-1)表示为S，S的定义是所有hash值小于θ值的hash元素集合。那么|S|就是所有hash值小于θ值的hash元素的数量。则TSF的估值函数就是|S|/θ。

![](777.png)

还是用之前的示例，输入流中不重复元素个数为10，k=3，那么在TSF中，θ为Value(k+1)即0.386，S为所有小于0.386的hash值集合，所以|S|=3，估值为3/0.386。
同样在hash数量小于k时，例如为2时，也就是输入流中不重复元素个数为2时，此时|S|=2，θ值还是初始值为1.0，那么估值就是2/1.0=2。

### 2 DataSketches Theta Sketch语法
#### 2.1数据摄入


Aggregators：

```
{
"type" : "thetaSketch", 
"name" : <output_name>, 
"fieldName" : <metric_name>, 
"isInputThetaSketch": false, 
"size": 16384 
}
```


| property           | description                                                  | required?     |
| ------------------ | ------------------------------------------------------------ | ------------- |
| type               | 总是"thetaSketch"                                            | 是            |
| name               | 输出字段名称(String)                                         | 是            |
| fieldName          | 输入字段名称(String)                                         | 是            |
| isInputThetaSketch | 只在摄入的数据中包含theta sketch objects时使用，即从druid之外的其他使用datasketchs库的系统向druid摄入数据的场景如：Pig/Hive | 否，默认false |
| size               | 2的倍数，代表要存储在theta sketch objects中的所有元素的个数。该值越大，准确度却高，需要消耗的存储空间也越大。当指定了某个特定的size值之后，druid会将sketch保存在segment中，查询只能使用>=该size的值。 推荐使用默认值 | 否，默认16384 |

示例：

海量日志按uicid去重，注意这里摄入的"fieldName":"uicIdNum"的值为具体的需要去重的id值，不是统计的数值：
```
"parser": {
      "type": "string",
      "parseSpec": {
        "format": "json",
        "timestampSpec": {
          "column": "fc_time",
          "format": "yyyy-MM-dd HH:mm:ss"
        },
        "dimensionsSpec": {
          "dimensions": [
            "bizDate",
			"hour",
            "project",
            "os",
            "eventId",
            "eventKey"
          ]
        }
      }
    },
    "metricsSpec": [
      {
        "name": "uicIdNum",
        "fieldName":"uicIdNum",
        "type": "thetaSketch"
      }
    ]
```

![](企业微信截图_1.png)


#### 2.2数据查询

##### 2.2.1sql查询：
sql查询Sketch的列，使用**APPROX_COUNT_DISTINCT_DS_THETA**函数

![企业微信截图_15706191401896](d:\Users\002616\My Pictures\企业微信截图_15706191401896.png)

示例：
```
SELECT __time,os,APPROX_COUNT_DISTINCT_DS_THETA(uicIdNum) as uicIdNum
FROM "huge_log_funnel_analysis"
WHERE "__time" >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
GROUP BY __time,os
```
![](企业微信截图_2.png)
##### 2.2.2json查询：
按小时去重ios人数：
```
{
  "queryType": "groupBy",
  "dataSource": "huge_log_funnel_analysis",
  "granularity": "hour",
  "dimensions": [],
  "aggregations": [
    { "type": "thetaSketch", "name": "uicIdNum", "fieldName": "uicIdNum" }
  ],
  "filter": { "type": "selector", "dimension": "os", "value": "ios" },
  "intervals": [ "2019-10-17T00:00:00.000Z/2019-10-18T00:00:00.000Z" ]
}
```

![](企业微信截图_4.png)

#### 2.3高级查询

通过Post Aggregators可实现UNION，INTERSECT，NOT查询

##### 2.3.1 UNION查询：

求android和ios使用人数并集：
```
{
  "queryType": "groupBy",
  "dataSource": "huge_log_funnel_analysis",
  "granularity": "hour",
  "dimensions": [],
  "filter": {
    "type": "or",
    "fields": [
      {"type": "selector", "dimension": "os", "value": "android"},
      {"type": "selector", "dimension": "os", "value": "ios"}
    ]
  },
  "aggregations": [
    {
      "type" : "filtered",
      "filter" : {
        "type" : "selector",
        "dimension" : "os",
        "value" : "android"
      },
      "aggregator" :     {
        "type": "thetaSketch", "name": "uicIdNumAndroid", "fieldName": "uicIdNum"
      }
    },
    {
      "type" : "filtered",
      "filter" : {
        "type" : "selector",
        "dimension" : "os",
        "value" : "ios"
      },
      "aggregator" :     {
        "type": "thetaSketch", "name": "uicIdNumIos", "fieldName": "uicIdNum"
      }
    }
  ],
  "postAggregations": [
    {
      "type": "thetaSketchEstimate",
      "name": "uicIdNum",
      "field":
      {
        "type": "thetaSketchSetOp",
        "name": "uicIdNum",
        "func": "UNION",
        "fields": [
          {
            "type": "fieldAccess",
            "fieldName": "uicIdNumAndroid"
          },
          {
            "type": "fieldAccess",
            "fieldName": "uicIdNumIos"
          }
        ]
      }
    }
  ],
  "intervals": [
    "2019-10-17T00:00:00.000Z/2019-10-18T00:00:00.000Z"
  ]
}
```
![](企业微信截图_5.png)
##### 2.3.2 INTERSECT：
android，ios使用人数交集

```
"postAggregations": [
    {
      "type": "thetaSketchEstimate",
      "name": "uicIdNum",
      "field":
      {
        "type": "thetaSketchSetOp",
        "name": "uicIdNum",
        "func": "INTERSECT",
        "fields": [
          {
            "type": "fieldAccess",
            "fieldName": "uicIdNumAndroid"
          },
          {
            "type": "fieldAccess",
            "fieldName": "uicIdNumIos"
          }
        ]
      }
    }
  ],
```
![](企业微信截图_6.png)

##### 2.3.3 NOT：
只使用android不使用ios的人数，刚好等于android使用人数- 交集的人数
```
"postAggregations": [
    {
      "type": "thetaSketchEstimate",
      "name": "uicIdNum",
      "field":
      {
        "type": "thetaSketchSetOp",
        "name": "uicIdNum",
        "func": "NOT",
        "fields": [
          {
            "type": "fieldAccess",
            "fieldName": "uicIdNumAndroid"
          },
          {
            "type": "fieldAccess",
            "fieldName": "uicIdNumIos"
          }
        ]
      }
    }
  ],
```

![](企业微信截图_7.png)

#### 2.4关于size

size必须为2的倍数，size的大小>=一个统计维度内所有不重复的数值的总个数

举例:按天，按终端类型(android/os)统计uv数据。如果一天内android的uv数预计为1000，os的uv数预计为900，则size数可设置为1024。thetaSketch是将android和ios两部分的用户id分别存储在segment里，如果合并查询os和android的uv数，thetaSketch会将两部分数据合并去重。

size设置过小会导致去重结果不准确，使用APPROX_COUNT_DISTINCT_DS_THETA
函数查询结果会偏大，使用json查询会出现浮点位。

![](Image.png)

默认数据摄入和查询的size数为16384，使用默认值可不设置size参数。如果需要手动指定size的大小，那么在摄入时需要设置size的具体值，如：`{"type" : "thetaSketch", 
"name" :"uv", 
"fieldName" :"uid", 
"size":  32768`}, 注意这种情况下在使用查询语句时也必须指定size的大小，并且size的大小需>=32768。如: `APPROX_COUNT_DISTINCT_DS_THETA(uv, 65536)`, `{
      "name": "visit",
      "type": "thetaSketch",
      "fieldName": "visit",
      "size":65536
    }`
    

#### 参考文献：

1，druid.io官网 [https://druid.apache.org/docs/latest/design/](https://druid.apache.org/docs/latest/design/)

2，DataSketches官网 [https://datasketches.github.io/docs/Theta/ThetaSketchFramework.html](https://datasketches.github.io/docs/Theta/ThetaSketchFramework.html)

3，[https://www.infoq.com/news/2016/01/Yahoo-open-sources-data-sketches/](https://www.infoq.com/news/2016/01/Yahoo-open-sources-data-sketches/)