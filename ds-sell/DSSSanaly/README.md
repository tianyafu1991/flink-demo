# flink-电商实时分析系统

### kafka0.10.0.X相关命令(CDH版本)
- 创建kafka topic
```
kafka-topics --create --zookeeper dsd:2181,dse:2181,dsf:2181 --replication-factor 2 --partitions 2 --topic flink-kafka-msi
kafka-topics --create --zookeeper dsd:2181,dse:2181,dsf:2181 --replication-factor 2 --partitions 2 --topic flink-kafka-output-msi
```
- 查看topic信息
```
kafka-topics --describe --zookeeper dsd:2181,dse:2181,dsf:2181 --topic flink-kafka-msi
```
   
- 生产者
```
kafka-console-producer --broker-list dsd:9092,dse:9092,dsf:9092 --topic flink-kafka-msi
```
    
- 消费者
```
kafka-console-consumer --zookeeper dsd:2181,dse:2181,dsf:2181 --topic flink-kafka-msi --from-beginning
kafka-console-consumer --zookeeper dsd:2181,dse:2181,dsf:2181 --topic flink-kafka-output-msi --from-beginning
```