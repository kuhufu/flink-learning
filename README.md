# 本地跑flink遇到的坑

1. 添加flink目录下的lib文件夹到项目
2. maven中库的版本要和flink的一致，不然会出现奇怪的错误
3. 不要把scala sdk的库添加到项目
4. 注意job编译时的jdk版本不要高于flink服务的jdk版本


### 主键
Flink 不存储数据因此只支持 NOT ENFORCED 模式，即不做检查，用户需要**自己保证唯一性**。

Flink 假设声明了主键的列都是不包含 Null 值的，Connector 在处理数据时需要**自己保证语义正确**。