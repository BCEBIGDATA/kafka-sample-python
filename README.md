# 百度Kafka服务python样例

百度Kafka是托管的Kafka消息服务。完全兼容开源Kafka。本样例展示如何使用官方推荐的confluent-kafka-python客户端访问百度Kafka服务。

## 环境要求

- [Python 2.7.x或Python 3.x](https://www.python.org/)
- [librdkafka 0.9.1以上](https://github.com/edenhill/librdkafka)
- [confluent-kafka-python 0.9.2及以上(支持kafka 0.10)](https://github.com/edenhill/librdkafka)

## 准备工作

细节请参考[BCE官网帮助文档](https://cloud.baidu.com/doc/Kafka/QuickGuide.html)

1. 在管理控制台中创建好主题，并获取主题名称topic_name。
2. 在管理控制台中下载您的kafka-key.zip，包含Go程序使用的`client.pem`，`client.key`，`ca.pem`。
3. 用上一步的文件替换样例代码中的`client.pem`、`client.key`以及`ca.pem`。

## 运行样例代码

	python consumer.py --topic topic_name

	python producer.py --topic topic_name

## 参考链接

- [百度Kafka产品介绍](https://bce.baidu.com/product/kafka.html)
- [Kafka](http://kafka.apache.org/)
- [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python)
- [librdkafka](https://github.com/edenhill/librdkafka)
- [php-rdkafka文档](https://arnaud-lb.github.io/php-rdkafka/phpdoc)