# learning-flink 大讲台

### 基本概念
- HBase相关操作
```$xslt
创建namespaces: 
create_namespace 'learning_flink'

创建表：
create 'learning_flink:users',{NAME => 'F'}

```
   
- 遇到的坑
```$xslt
1.本地操作远程hbase时  需要在类路径下有hbase-site.xml文件，否则连接zk时会连接localhost的zk，而不是远程的zk
```
    
- Time