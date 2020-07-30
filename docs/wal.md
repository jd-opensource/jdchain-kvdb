### WAL

#### 存储结构

1. `meta`

- `file`: `wal`文件索引
- `lsn`: 已入库的最大序号

元信息结构化为[`MetaInfo`](#https://git.jd.com/jd-blockchain/kvdb/blob/feature/wal/kvdb-server/src/main/java/com/jd/blockchain/kvdb/server/wal/MetaInfo.java)，使用`JD Chain`序列化框架进行序列化/反序列化操作，每次更新覆盖写入。

2. `wal`

```img
   header          entity           header  
[ - - - - ][ - - - ...... - - - ][ - - - - ]
  4 bytes	      n bytes		   4 bytes
```
- `hader`: 存放`entity`数据大小，`4`字节
- `entity`: 存放操作日志

日志结构化为[`WalEntity`](#https://git.jd.com/jd-blockchain/kvdb/blob/feature/wal/kvdb-server/src/main/java/com/jd/blockchain/kvdb/server/wal/WalEntity.java)，使用`JD Chain`序列化框架进行序列化/反序列化操作，每次`append`内容由`header`+`entity`+`header`组成，`header`四个字节，里面存放`entity`数据大小，第二个冗余`header`主要用于逆向读取。

#### 操作流程

1. 数据库操作
```img
wal append
rocksdb batch commit
meta update
```

2. 服务端重启
```img
meta lsn < wal latest lsn ?
    yes:
        turn off wal
        iterator wal lsn > meta lsn {
            redo db operation
        }
        reset wal
        update meta
        start server
    no:
        start server
```

#### 配置

1. `wal.disable`

是否关闭`wal`，默认`false`

2. `wal.flush`
`wal`刷新（`fsync`）机制：
- `-1`: 跟随系统
- `0`: 实时异步刷新
- 大于`0`的数值`n`: 定时`n`刷新一次

#### 滚动/删除

TODO

设置单个`wal`文件大小最大值进行滚动，删除距离当前超过某一时间的历史日志。