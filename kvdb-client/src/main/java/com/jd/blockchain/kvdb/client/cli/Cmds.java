package com.jd.blockchain.kvdb.client.cli;

import com.jd.blockchain.kvdb.client.KVDBClient;
import com.jd.blockchain.kvdb.protocol.ClusterInfo;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.utils.Bytes;
import com.jd.blockchain.utils.io.BytesUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.ExitRequest;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.commands.Quit;

import java.util.Arrays;

@ShellComponent
public class Cmds implements Quit.Command {

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

    @ShellMethod(group = "KVDB Commands",
            value = "Server information.")
    public String info() throws KVDBException {
        com.jd.blockchain.kvdb.protocol.Info info = client.info();
        StringBuilder builder = new StringBuilder();
        builder.append("mode: ");
        builder.append(info.isClusterMode() ? "cluster" : "single");
        builder.append("\n");
        if (info.isClusterMode()) {
            builder.append("cluster: \n");
            ClusterInfo[] clusterInfos = info.getCluster();
            for (int i = 0; i < clusterInfos.length; i++) {
                builder.append("    " + clusterInfos[i].getName() + ": " + Arrays.toString(clusterInfos[i].getURLs()));
                if (i != clusterInfos.length - 1) {
                    builder.append("\n");
                }
            }
        }

        return builder.toString();

    }

    @ShellMethod(group = "KVDB Commands",
            value = "Show databases",
            key = "show databases")
    public String showDatabases() throws KVDBException {
        String[] names = client.showDatabases();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < names.length; i++) {
            builder.append(names[i]);
            if (i != names.length - 1) {
                builder.append("\n");
            }
        }

        return builder.toString();

    }

    @ShellMethod(group = "KVDB Commands",
            value = "Set a key-value",
            key = {"set", "put"})
    public boolean put(String key, String value) throws KVDBException {

        return client.put(Bytes.fromString(key), Bytes.fromString(value));
    }

    @ShellMethod(group = "KVDB Commands",
            value = "Get value")
    public String get(String key) throws KVDBException {

        Bytes value = client.get(Bytes.fromString(key));
        return null != value ? BytesUtils.toString(value.toBytes()) : "not exists";
    }

    @ShellMethod(group = "KVDB Commands",
            value = "Check for existence")
    public boolean exists(String key) throws KVDBException {

        return client.exists(Bytes.fromString(key));
    }

    @ShellMethod(group = "KVDB Commands",
            value = "Create a database use the giving name",
            key = "create database")
    public boolean createDB(String name) throws KVDBException {

        return client.createDatabase(name);
    }

    @ShellMethod(group = "KVDB Commands",
            value = "Switch to the database with the specified name")
    public boolean use(String name) throws KVDBException {

        return client.use(name);
    }

    @ShellMethod(group = "KVDB Commands",
            value = "Start a batch",
            key = "batch begin")
    public boolean batchBegin() throws KVDBException {

        return client.batchBegin();
    }

    @ShellMethod(group = "KVDB Commands",
            value = "Abort the existing batch",
            key = "batch abort")
    public boolean batchAbort() throws KVDBException {

        return client.batchAbort();
    }

    @ShellMethod(group = "KVDB Commands",
            value = "Commit the existing batch",
            key = "batch commit")
    public boolean batchCommit() throws KVDBException {

        return client.batchCommit();
    }
}
