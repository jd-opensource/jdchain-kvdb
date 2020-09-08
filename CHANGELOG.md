# Changelog

## v1.1.0
TODO

**DOWNLOAD**

[下载链接](http://storage.jd.com/jd.block.chain/kvdb-1.1.0.RELEASE.zip)

**功能**
* 加入`wal`保证数据可靠写入
* 加入`kvdb-deploy`打包部署模块

**优化**
* 批量操作中，`SET`操作可选择非同步等待执行结果，而只在批量提交时校验`KV`数量。

## v1.0.1
2020.06.11

**DOWNLOAD**

[下载链接](http://storage.jd.com/jd.block.chain/kvdb-1.0.1.RELEASE.zip)

**BUG修复**
* 线程池为关闭导致`kvdb-cli`无法退出异常
* 测试用例不能重复执行

**优化**
* `RocksDB`参数调优

## v1.0.0
2020.05.16

**DOWNLOAD**

[下载链接](http://storage.jd.com/jd.block.chain/kvdb-1.0.0.RELEASE.zip)

**功能**
* `RocksDB`独立部署，提供集群配置，数据库操作，`KV`操作
* 单机`RocksDB`分片
* 服务集群部署