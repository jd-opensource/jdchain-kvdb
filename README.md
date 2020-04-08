## RocksDB as a server

### 简介

`KVDB`是一个简单的`NoSQL`数据库，支持简单的“键值”读写操作。

`KVDB`包装了`RocksDB`作为数据库引擎，实现了单机部署和集群部署。

`KVDB`的集群是一个分布式的分片服务集群，每个分片节点是一个`KVDB`的数据库服务实例，采用对等模式部署，没有主节点。

### 安装

> java 版本>= 1.8 

下载源代码，执行：
```bash
mvn clean package
```
`kvdb-server`模块`target`下会生成`kvdb-***.zip`，解压缩安装包，结构如下：
```bash
bin # 脚本文件目录
  start # 启动本机的数据库服务的脚本文件
  stop # 停止本机的数据库服务的脚本文件
  kvdb-cli # 连接本机的数据库服务控制台的脚本文件
  kvdb-benchmark # 基准测试
conf # 配置文件目录
  kvdb.conf # 本机的数据库服务配置
  cluster.conf # 数据库集群的配置
libs # 数据库服务的代码库目录
system # 系统数据目录
  dblist # 文件记录了本数据库服务实例装载的数据库列表
  pid # 文件记录了本数据库服务实例的运行进程ID；当服务实例运行时创建，服务实例退出后删除
```

### 部署运行

#### 单机模式

1. `kvdb.conf`配置：

```bash
# 数据库服务的本机监听地址；
server.host=0.0.0.0

# 数据库服务的本机监听端口；
server.port=7078

# 方式运行: cluster single
server.mode=single

# 管理控制台的端口；
# 注：管理控制台总是绑定到环回地址 127.0.0.1，只允许本机访问；
manager.port=7060

# 数据库实例默认的根目录
dbs.rootdir=/usr/kvdb/dbs

# 数据库实例默认的本地分区数
dbs.partitions=4
```

2. `dblist`配置：
```bash
# 是否激活数据库 ‘<name>’ ；如果为`true`，则创建或加载该数据库并提供访问；
# ‘<name>’表示数据库名称，只允许用字母、数字、下划线组成；
# db.<name>.enable=true

# 数据库 <name> 的根目录；如果未配置，则从默认目录加载(由`conf/kvdb.conf`指定了默认配置${dbs.rootdir}/<name>)；
# db.test.rootdir=

# 数据库 <name> 的本地分区数；如果未配置，则采用`conf/kvdb.conf`中指定的默认配置${dbs.partitions}
# db.test.partitions=
```
> `dblist`中配置的数据库会在`kvdb-server`启动时自动创建。用户可以不配置此文件，在`kvdb-server`启动完成后由客户端执行`create database <name>`创建数据库，创建成功的数据库会追加到`dblist`中。

3. 启动停止

执行`start`命令便可以以单机服务方式启动/停止`kvdb-server`：
```bash
# 启动
./start.sh
# 停止
./stop.sh
```

#### 集群模式

1. `kvdb.conf`配置

与[单机模式](#单机模式)主要区别在运行方式上：
```bash
...
# 方式运行: cluster single
server.mode=single
...
```
> 如果在同一台机器上部署多个集群节点，`server.port`和`manager.port`注意做响应更改。

2. `dblist`配置

与[单机模式](#单机模式)一致，唯一需要注意的是`cluster.conf`中配置的数据库（`dbname`）必须在`dblist`中设置为`true`，即`kvdb-server`启动后会创建或加载该数据库并提供访问。

3. `cluster.conf`配置：
```bash
# 数据库集群的分片数，每一个分片都赋予唯一的编号，分片编号最小为 0，所有分片的编号必须连续递增
# ‘<name>’表示集群名称，只允许用字母、数字、下划线组成；
cluster.<name>.partitions=3

# 数据库集群 ‘<name>’ 的第 1 个分片的数据库实例地址(URL格式)；
cluster.<name>.0=kvdb://host:port/<dbname>

# 数据库集群 ‘<name>’ 的第 2 个分片的数据库实例地址(URL格式)；
cluster.<name>.1=kvdb://host:port/<dbname>

# 数据库集群 ‘<name>’ 的第 3 个分片的数据库实例地址(URL格式)；
cluster.<name>.2=kvdb://homt:port/<dbname>

# 指定多个不同的集群
#cluster.<name1>.partitions=3
...
```
> 一个数据库实例只能加入唯一的集群；一旦加入集群，则不能再被客户端以单库连接方式访问，对该库实例的连接自动转为集群连接。

> 0.6.0版本仅在所有节点提供完全一致的`cluster.conf`配置才能正常运行

4. 启动停止

每个集群节点启动停止方式和[单机模式](#单机模式)一致。
节点启动过程中会同步其他所有节点集群配置信息，只有所有节点集群配置完全一致，服务集群才能提供正常服务。

### SDK

1. 依赖
```maven
<dependency>
	<groupId>com.jd.blockchain</groupId>
	<artifactId>kvdb-client</artifactId>
	<version>${version}</version>
</dependency>
```

2. 创建客户端连接
```java
// `host`、`port`为服务器地址和端口，可以是单机或者集群的任意节点
// `database`为使用的数据库名称，[单机模式](#单机模式)时与`dblist`中`<name>`配置项一致；[集群模式](#集群模式)时与`cluster.conf`中`<name>`即集群名称一致。
KVDBClient client = new KVDBClient("kvdb://<host>:<port>/<database>");
```

3. 操作

```java
/**
 * KVDB SDK 所有支持操作 
 */
public interface KVDBOperator {

		// 关闭客户端连接
    void close();
    
		// 切换数据库/集群
    boolean use(String db) throws KVDBException;
    
		// 创建数据库，集群模式下不支持此操作
    boolean createDB(String db) throws KVDBException;
    
		// 服务器信息，`0.6.0`返回服务器单机/集群方式以及集群完整配置
    Info info() throws KVDBException;
    
		// 显示所有数据库/集群
    String[] showDBs() throws KVDBException;
    
		// 是否存在某个键值
    boolean exists(Bytes key) throws KVDBException;
    
		// 查询多个键存在性
    boolean[] exists(Bytes... keys) throws KVDBException;
    
		// 获取键值
    Bytes get(Bytes key) throws KVDBException;
    
		// 获取多个键值
    Bytes[] get(Bytes... keys) throws KVDBException;
    
		// 设置键值对，支持一次多个键值对操作以`key value key value ...`即`key`，`value`交替出现的方式提交
    boolean put(Bytes... kvs) throws KVDBException;
    
		// 开启`batch`
    boolean batchBegin() throws KVDBException;
    
		// 取消`batch`
    boolean batchAbort() throws KVDBException;
    
		// 提交`batch`，未提交的`batch`对其他客户端连接不可见。
    boolean batchCommit() throws KVDBException;
}
```
> `KVDBClient`会自动识别所连接的服务是单机还是集群模式，客户端无需所任何额外操作。

> 集群模式下客户端会创建与所有服务节点的连接，针对不同的`key`值操作会路由到不同的集群节点。

> 集群模式下不支持创建数据库，需要在服务节点启动前在`dblist`和`cluster.conf`中创建并配置好需要使用的集群和数据库。

### 命令行

`kvdb-cli`是基于[`SDK`](#SDK)的命令行工具实现：

```bash
./kvdb-cli.sh -h <kvdb server host> -p <kvdb server port> -db <database> -t <time out in milliseconds>  -rt <retry times for time out> -bs <buffer size> -k <keep alive>
```
参数说明：

- `-h` 服务器地址。选填，默认`localhost`
- `-p` 端口。选填，默认`7078`
- `-db` 数据库/集群名称。选填
- `-t` 超时时间，毫秒。选填，默认`60000 ms`
- `-rt` 超市重试等待次数。选填，默认`5`
- `-bs` 发送/接收缓冲区大小。选填，默认`1024*1024`
- `-k` 保持连接。选填，默认`true`

所有支持指令操作：
```bash
localhost:7080>help
AVAILABLE COMMANDS

Built-In Commands
        clear: Clear the shell screen.
        exit, quit: Exit the shell.
        help: Display help about available commands.
        script: Read and execute commands from a file.
        stacktrace: Display the full stacktrace of the last error.

KVDB Commands
        batch abort: Abort the existing batch
        batch begin: Start a batch
        batch commit: Commit the existing batch
        create database: Create a database use the giving name
        exists: Check for existence
        get: Get value
        info: Server information.
        put, set: Set a key-value
        show databases: Show databases
        use: Switch to the database with the specified name
```
> `0.6.0`版本`kvdb-cli`仅支持单键值对操作。