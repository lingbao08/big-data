
#说明

该项目包含SSF（Spark、Storm、Flink）代码。


### spark-streaming-eg

这是第一个用Scala的作品。哈哈，略显拙劣。

first use, first project

大致需求是通过对日志的分析，了解每个地方去的次数。

分为3个模块：common，spark-show（web展示），spark-streaming-eg（采集数据）。

spark内部使用了java和Scala来实现，数据从日志文件到flume，在经过kafka，数据持久化到HBASE中。

show-going采用echarts来进行展示。

### flink-eg

这是一个flink数据处理代码。没有展示页面等。


### storm-eg

这是一个storm数据处理代码。没有展示页面等。






















