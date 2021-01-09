package com.jd.blockchain.kvdb.cli;

import com.jd.blockchain.kvdb.client.KVDBClient;
import com.jd.blockchain.kvdb.protocol.client.ClientConfig;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.kvdb.protocol.proto.ClusterItem;
import com.jd.blockchain.kvdb.protocol.proto.DatabaseBaseInfo;
import com.jd.blockchain.kvdb.protocol.proto.DatabaseClusterInfo;
import com.jd.blockchain.kvdb.protocol.proto.impl.KVDBDatabaseBaseInfo;

import utils.Bytes;
import utils.StringUtils;
import utils.io.BytesUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.shell.ExitRequest;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;
import org.springframework.shell.standard.commands.Quit;

import static org.springframework.shell.standard.ShellOption.NULL;

@ShellComponent
public class Cmds implements Quit.Command {

    private static final Logger LOGGER = LoggerFactory.getLogger(Cmds.class);

    final ClientConfig config;
    private KVDBClient client;

    public Cmds(ClientConfig config) {
        this.config = config;
        try {
            this.client = new KVDBClient(config);
        } catch (KVDBException e) {
            LOGGER.error("client start error", e);
            System.out.println("error: " + e.getMessage());
            if (null != client) {
                client.close();
            }
            System.exit(0);
        }
    }

    @ShellMethod(
            group = "Built-In Commands",
            value = "Exit the shell.",
            key = {"quit", "exit"}
    )
    public void quit() {
        client.close();
        throw new ExitRequest();
    }

    /**
     * 当前状态，目前仅显示客户端连接数据库名称
     *
     * @return
     * @throws KVDBException
     */
    @ShellMethod(group = "KVDB Commands",
            value = "Current database information.",
            key = "status")
    public String statusCmd() throws KVDBException {
        StringBuilder builder = new StringBuilder();
        if (!StringUtils.isEmpty(config.getDatabase())) {
            builder.append("database: ");
            builder.append(config.getDatabase());
        } else {
            builder.append("no database selected");
        }

        return builder.toString();

    }

    /**
     * 服务器集群配置信息，展示所有集群及其对应的服务节点列表
     *
     * @return
     * @throws KVDBException
     */
    @ShellMethod(group = "KVDB Commands",
            value = "Server cluster information.",
            key = "cluster info")
    public String clusterInfoCmd() {
        try {
            ClusterItem[] infos = client.clusterInfo();
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < infos.length; i++) {
                builder.append(infos[i].getName());
                builder.append(": \n");
                String[] urls = infos[i].getURLs();
                for (int j = 0; j < urls.length; j++) {
                    builder.append("    " + urls[j]);
                    builder.append("\n");
                }
                if (i != infos.length - 1) {
                    builder.append("\n");
                }
            }

            return builder.toString();
        } catch (KVDBException e) {
            return "error: " + e.getMessage();
        }

    }

    /**
     * 显示当前服务器所有可提供服务的数据库名称
     *
     * @return
     * @throws KVDBException
     */
    @ShellMethod(group = "KVDB Commands",
            value = "Show databases",
            key = "show databases")
    public String showDatabasesCmd() {
        try {
            DatabaseBaseInfo[] dbs = client.showDatabases();
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < dbs.length; i++) {
                DatabaseBaseInfo info = dbs[i];
                builder.append(info.getName() + ":");
                builder.append("\n    enable:" + info.isEnable());
                builder.append("\n    rootdir:" + info.getRootDir());
                builder.append("\n    partitions:" + info.getPartitions());
                if (i != dbs.length - 1) {
                    builder.append("\n");
                }
            }
            return builder.toString();
        } catch (KVDBException e) {
            return "error: " + e.getMessage();
        }
    }

    /**
     * 设置key-value
     *
     * @param key
     * @param value
     * @return
     * @throws KVDBException
     */
    @ShellMethod(group = "KVDB Commands",
            value = "Set a key-value",
            key = {"set", "put"})
    public String putCmd(String key, String value) {
        try {
            return client.put(Bytes.fromString(key), Bytes.fromString(value)) ? "true" : "false";
        } catch (KVDBException e) {
            return "error: " + e.getMessage();
        }
    }

    /**
     * 查询键值
     *
     * @param key
     * @return
     * @throws KVDBException
     */
    @ShellMethod(group = "KVDB Commands",
            value = "Get value",
            key = "get")
    public String getCmd(String key) {
        try {
            Bytes value = client.get(Bytes.fromString(key));
            return null != value ? BytesUtils.toString(value.toBytes()) : "not exists";
        } catch (KVDBException e) {
            return "error: " + e.getMessage();
        }
    }

    /**
     * 存在性查询
     *
     * @param key
     * @return
     * @throws KVDBException
     */
    @ShellMethod(group = "KVDB Commands",
            value = "Check for existence",
            key = "exists")
    public String existsCmd(String key) {
        try {
            return client.exists(Bytes.fromString(key)) ? "true" : "false";
        } catch (KVDBException e) {
            return "error: " + e.getMessage();
        }
    }

    /**
     * 在当前连接{@link Cmds#config}服务器创建数据库
     *
     * @param name
     * @return
     * @throws KVDBException
     */
    @ShellMethod(group = "KVDB Commands",
            value = "Create a database use the giving name",
            key = "create database")
    public String createDatabaseCmd(String name, @ShellOption(defaultValue = NULL) Integer partitions) {
        try {
            if (null == partitions) {
                partitions = 0;
            } else if (partitions < 0) {
                throw new KVDBException("partitions can not be negative");
            }
            return client.createDatabase(new KVDBDatabaseBaseInfo(name, partitions)) ? "true" : "false";
        } catch (KVDBException e) {
            return "error: " + e.getMessage();
        }
    }

    /**
     * 在当前连接{@link Cmds#config}服务器开启数据库实例
     *
     * @param name
     * @return
     * @throws KVDBException
     */
    @ShellMethod(group = "KVDB Commands",
            value = "enable database",
            key = "enable database")
    public String enableDatabaseCmd(String name) {
        try {
            return client.enableDatabase(name) ? "true" : "false";
        } catch (KVDBException e) {
            return "error: " + e.getMessage();
        }
    }

    /**
     * 在当前连接{@link Cmds#config}服务器关闭数据库实例
     *
     * @param name
     * @return
     * @throws KVDBException
     */
    @ShellMethod(group = "KVDB Commands",
            value = "disable database",
            key = "disable database")
    public String disableDatabaseCmd(String name) {
        try {
            boolean ok = client.disableDatabase(name);
            if (ok && name.equals(config.getDatabase())) {
                config.setDatabase(null);
            }

            return ok ? "true" : "false";
        } catch (KVDBException e) {
            return "error: " + e.getMessage();
        }
    }

    /**
     * 在当前连接{@link Cmds#config}服务器关闭数据库实例
     *
     * @param name
     * @return
     * @throws KVDBException
     */
    @ShellMethod(group = "KVDB Commands",
            value = "drop database",
            key = "drop database")
    public String dropDatabaseCmd(String name) {
        try {
            boolean ok = client.dropDatabase(name);
            if (ok && name.equals(config.getDatabase())) {
                config.setDatabase(null);
            }

            return ok ? "true" : "false";
        } catch (KVDBException e) {
            return "error: " + e.getMessage();
        }
    }

    /**
     * 切换数据库，并打印已切换数据库信息
     *
     * @param name
     * @return
     * @throws KVDBException
     * @throws InterruptedException
     */
    @ShellMethod(group = "KVDB Commands",
            value = "Switch to the database with the specified name",
            key = "use")
    public String useCmd(String name) {
        try {
            DatabaseClusterInfo info = client.use(name);
            StringBuilder builder = new StringBuilder();
            builder.append("mode: ");
            builder.append(info.isClusterMode() ? "cluster" : "single");
            if (info.isClusterMode()) {
                builder.append("\n");
                ClusterItem cluster = info.getClusterItem();
                builder.append(cluster.getName());
                builder.append(": \n");
                String[] urls = cluster.getURLs();
                for (int i = 0; i < urls.length; i++) {
                    builder.append("    " + urls[i]);
                    if (i != urls.length - 1) {
                        builder.append("\n");
                    }
                }
            }
            return builder.toString();
        } catch (KVDBException e) {
            return "error: " + e.getMessage();
        }
    }

    /**
     * 开启批处理
     *
     * @return
     * @throws KVDBException
     */
    @ShellMethod(group = "KVDB Commands",
            value = "Start a batch",
            key = "batch begin")
    public String batchBeginCmd() {
        try {
            return client.batchBegin() ? "true" : "false";
        } catch (KVDBException e) {
            return "error: " + e.getMessage();
        }
    }

    /**
     * 取消批处理
     *
     * @return
     * @throws KVDBException
     */
    @ShellMethod(group = "KVDB Commands",
            value = "Abort the existing batch",
            key = "batch abort")
    public String batchAbortCmd() {
        try {
            return client.batchAbort() ? "true" : "false";
        } catch (KVDBException e) {
            return "error: " + e.getMessage();
        }
    }

    /**
     * 提交批处理
     *
     * @return
     * @throws KVDBException
     */
    @ShellMethod(group = "KVDB Commands",
            value = "Commit the existing batch",
            key = "batch commit")
    public String batchCommitCmd() {
        try {
            return client.batchCommit() ? "true" : "false";
        } catch (KVDBException e) {
            return "error: " + e.getMessage();
        }
    }
}
