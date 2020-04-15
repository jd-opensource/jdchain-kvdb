package com.jd.blockchain.kvdb.client.cli;

import com.jd.blockchain.kvdb.client.KVDBClient;
import com.jd.blockchain.kvdb.protocol.ClusterItem;
import com.jd.blockchain.kvdb.protocol.DatabaseInfo;
import com.jd.blockchain.kvdb.protocol.client.ClientConfig;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.kvdb.protocol.parameter.KVDBCreateDatabaseParam;
import com.jd.blockchain.utils.Bytes;
import com.jd.blockchain.utils.StringUtils;
import com.jd.blockchain.utils.io.BytesUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.ExitRequest;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;
import org.springframework.shell.standard.commands.Quit;

import java.util.Arrays;

import static org.springframework.shell.standard.ShellOption.NULL;

@ShellComponent
public class Cmds implements Quit.Command {

    @Autowired
    ClientConfig config;

    @Autowired
    KVDBClient client;

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
    public String status() throws KVDBException {
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
    public String clusterInfo() throws KVDBException {
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
    public String showDatabases() throws KVDBException {
        String[] names = client.showDatabases();
        Arrays.sort(names);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < names.length; i++) {
            builder.append(names[i]);
            if (i != names.length - 1) {
                builder.append("\n");
            }
        }

        return builder.toString();

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
    public boolean put(String key, String value) throws KVDBException {

        return client.put(Bytes.fromString(key), Bytes.fromString(value));
    }

    /**
     * 查询键值
     *
     * @param key
     * @return
     * @throws KVDBException
     */
    @ShellMethod(group = "KVDB Commands",
            value = "Get value")
    public String get(String key) throws KVDBException {

        Bytes value = client.get(Bytes.fromString(key));
        return null != value ? BytesUtils.toString(value.toBytes()) : "not exists";
    }

    /**
     * 存在性查询
     *
     * @param key
     * @return
     * @throws KVDBException
     */
    @ShellMethod(group = "KVDB Commands",
            value = "Check for existence")
    public boolean exists(String key) throws KVDBException {

        return client.exists(Bytes.fromString(key));
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
    public boolean createDatabase(String name, @ShellOption(defaultValue = NULL) String rootDir, @ShellOption(defaultValue = NULL) Integer partitions) throws KVDBException {
        if (StringUtils.isEmpty(name)) {
            throw new KVDBException("database name can not be empty");
        }
        if (null == rootDir) {
            rootDir = "";
        }
        if (null == partitions) {
            partitions = 0;
        } else if (partitions < 0) {
            throw new KVDBException("partitions can not be negative");
        }
        return client.createDatabase(new KVDBCreateDatabaseParam(name, rootDir, partitions));
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
            value = "Switch to the database with the specified name")
    public String use(String name) throws KVDBException, InterruptedException {
        DatabaseInfo info = client.use(name);
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
    public boolean batchBegin() throws KVDBException {

        return client.batchBegin();
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
    public boolean batchAbort() throws KVDBException {

        return client.batchAbort();
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
    public boolean batchCommit() throws KVDBException {

        return client.batchCommit();
    }
}
