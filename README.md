# 本地跑flink遇到的坑

1. 添加flink目录下的lib文件夹到项目
2. maven中库的版本要和flink的一致，不然会出现奇怪的错误
3. 不要把scala sdk的库添加到项目
4. 注意job编译时的jdk版本不要高于flink服务的jdk版本