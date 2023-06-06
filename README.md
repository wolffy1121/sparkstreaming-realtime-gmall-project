# sparkstreaming-realtime-gmall-project
sparkstreaming-电商实时项目,scala语言开发

> 错误：
> Caused by: java.util.MissingResourceException: Can't find resource for bundle java.util.PropertyResourceBundle, key kafka.bootstrap.servers
> 读取的key与properties文件中的key不一致，大概率是写错了
> 检查:kafka.bootstrap.servers

OdsBaseLogAPP.scala

日志-->kafka:ODS_BASE_LOG-->sparkstreaming-->kafka:DWD_START_LOG,DWD_PAGE_LOG,DWD_PAGE_ACTION,DWD_PAGE_DISPLAY

>Could not get a resource since the pool is exhausted  
> 更改配置文件 redis.conf 中两处   
> 1.将 bind 127.0.0.1 注释掉  
> 2.将 protected-mode 改为 no  
> 3.讲 daemonize 改为 yes  
> 4.jedis maven版本也可能出现问题

MISCONF Redis is configured to save RDB snapshots, but it is currently not able to persist on disk. Commands that may modify the data set are disabled, because this instance is configured to report errors during writes if RDB snapshotting fails (stop-writes-on-bgsave-error option). Please check the Redis logs for details about the RDB error.
原因：不能持久化问题
通过redis命令行修改
config set stop-writes-on-bgsave-error no




提交Offset位置：MyOffsetUtils.saveOffset(ods_base_topic, group_id, offsetRanges)    
foreahRDD外面 driver中执行，一次启动执行一次   
foreachRDD里边  driver中执行，一个批次执行一次   
foreach里边  executor中执行，一条数据执行一次     