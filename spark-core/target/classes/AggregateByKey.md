## aggregateByKey 图解
当调用（K，V）对的数据集时，返回（K，U）对的数据集，其中使用给定的组合函数和 zeroValue 聚合每个键的值。
与 groupByKey 类似，reduce 任务的数量可通过第二个参数 numPartitions 进行配置。示例如下：

```scala
// 为了清晰，以下所有参数均使用具名传参
val list = List(("hadoop", 3), ("hadoop", 2), ("spark", 4), ("spark", 3), ("storm", 6), ("storm", 8))
sc.parallelize(list,numSlices = 2).aggregateByKey(zeroValue = 0,numPartitions = 3)(
      seqOp = math.max(_, _),
      combOp = _ + _
    ).collect.foreach(println)
//输出结果：
(hadoop,3)
(storm,8)
(spark,7)
```
这里使用了 numSlices = 2 指定 aggregateByKey 父操作 parallelize 的分区数量为 2，其执行流程如下：

![流程图解](https://camo.githubusercontent.com/2f2a882b43c108894437d88d8fd0611727451c915eb6011225f2635ee28f0d04/68747470733a2f2f67697465652e636f6d2f68656962616979696e672f426967446174612d4e6f7465732f7261772f6d61737465722f70696374757265732f737061726b2d61676772656761746542794b65792e706e67)

基于同样的执行流程，如果 numSlices = 1，则意味着只有输入一个分区，则其最后一步 combOp 相当于是无效的，执行结果为：
```scala
(hadoop,3)
(storm,8)
(spark,4)
```

同样的，如果每个单词对一个分区，即 numSlices = 6，此时相当于求和操作，执行结果为：
```scala
(hadoop,5)
(storm,14)
(spark,7)
```
aggregateByKey(zeroValue = 0,numPartitions = 3) 的第二个参数 numPartitions 决定的是输出 RDD 的分区数量，想要验证这个问题，
可以对上面代码进行改写，使用 getNumPartitions 方法获取分区数量：

```scala
sc.parallelize(list,numSlices = 6).aggregateByKey(zeroValue = 0,numPartitions = 3)(
seqOp = math.max(_, _),
combOp = _ + _
).getNumPartitions
```
![输出RDD的分区数](https://camo.githubusercontent.com/319d8b4ba850538bca1987398c74812513070251cf00e25336dfa444ca50517f/68747470733a2f2f67697465652e636f6d2f68656962616979696e672f426967446174612d4e6f7465732f7261772f6d61737465722f70696374757265732f737061726b2d676574706172746e756d2e706e67)