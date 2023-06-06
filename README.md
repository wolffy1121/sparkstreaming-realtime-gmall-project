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