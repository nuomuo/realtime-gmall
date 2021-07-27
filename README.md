### Flink 实时项目
模块介绍：

| 模块名                 | 功能简介                                                     |
| ---------------------- | ------------------------------------------------------------ |
| **flink-gmall-logger** | 日志服务器：处理日期请求，将数据发往Kafka，并另存储一份到本地 |



### Spark-streaming 实时项目

模块介绍：

| 模块名              | 功能简介                                                  |
| ------------------- | --------------------------------------------------------- |
| gmall-canal         | canal 客户端 将 Mysql 变化的数据 发送到 kafka             |
| gmall-commom        | 想项目的通用数据，如 Kafka Topic 名、ElasticSearch 索引名 |
| gmall-elasticsearch | ElasticSearch Api 测试                                    |
| gmall-realtime      | 主函数入口                                                |
| gmall-webapi        | 数据可视化接口                                            |

具体需求实现和细节：

- 应用一：实时日活

  使用 Redis 作为中间缓存实现 批次间的去重

- 应用二：实时 GVM

  使用 Canal 实现 Mysql 业务数据的实时同步至 Kafka，在同步至 Hbase

- 应用三：领券行为异常实时监控

  Spark Streaming 处理实时数据（如何判定行为异常且生成预警信息 至 ElasticSearch）

- 应用四：实时灵活分析

  使用 Redis 实现 双流 join