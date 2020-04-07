## RocksDB as a server

### quick start

#### server

```bash
mvn clean package
```

```bash
java -jar kvdb-server.jar
```
use `-c config_file_absolute_path` to change the default configurations.

configs:
```bash
server.host=localhost
server.port=6380
buffer_size=1048576
db.path=db
// db size, use `select` command to change
db.size=4
// partitions for single db, values gt 1 use `RocksDBCluster`, otherwise use `RocksDBProxy`.
db.partition=4
```

#### cli

```bash
java -jar kvdb-cli.jar -h localhost -p 6380
```

What can you do:
```bash
localhost:6380>help
support commands:

 select : change database
 put : set key-value pairs
 get : get key-value pairs
 exists : check existence of keys
 batch-begin : begin batch
 batch-abort : abort batch
 batch-commit : commit batch
 help : i am the man
 quit : bye~
 
```

#### client

```java
// new cluster client
KVDBClient kvdbClient = new KVDBCluster(ClientConfig...;

// new single client
KVDBClient kvdbClient = new KVDBSingle(HOST, PORT);
KVDBClient kvdbClient = new KVDBSingle(ClientConfig);

kvdbClient.start();

// select db
kvdbClient.select(1);

// put k-v
kvdbClient.put(Bytes.fromString("k"), Bytes.fromString("v"));

// get k
Bytes value = kvdbClient.get(Bytes.fromString("k"));

// exists
kvdbClient.exists(Bytes.fromString("k"));

// batch begin
Client.batchBegin();

// batch abort
Client.batchAbort();

// batch commit
Client.batchCommit();

kvdbClient.stop();
```