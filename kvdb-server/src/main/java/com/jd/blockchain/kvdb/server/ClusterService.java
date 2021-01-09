package com.jd.blockchain.kvdb.server;

import com.jd.binaryproto.BinaryProtocol;
import com.jd.blockchain.kvdb.protocol.Constants;
import com.jd.blockchain.kvdb.protocol.KVDBURI;
import com.jd.blockchain.kvdb.protocol.client.ClientConfig;
import com.jd.blockchain.kvdb.protocol.client.NettyClient;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.kvdb.protocol.proto.ClusterInfo;
import com.jd.blockchain.kvdb.protocol.proto.ClusterItem;
import com.jd.blockchain.kvdb.protocol.proto.Response;
import com.jd.blockchain.kvdb.protocol.proto.impl.KVDBMessage;

import utils.io.BytesUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 集群相关操作
 */
public class ClusterService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterService.class);

    private static final int CLUSTER_CONFIRM_TIME_OUT = 10000;

    private ServerContext serverContext;

    public ClusterService(ServerContext serverContext) {
        this.serverContext = serverContext;
    }

    /**
     * 集群配置同步确认
     */
    public void confirm() throws KVDBException {
        boolean confirmed = false;
        ClusterInfo localClusterInfo = serverContext.getClusterInfo();
        if (localClusterInfo.size() == 0) {
            return;
        }
        while (!confirmed) {
            Set<String> confirmedHosts = new HashSet<>();
            for (ClusterItem entry : localClusterInfo.getClusterItems()) {
                boolean ok = true;
                for (String url : entry.getURLs()) {
                    KVDBURI uri = new KVDBURI(url);
                    if (!(uri.isLocalhost() && uri.getPort() == serverContext.getConfig().getKvdbConfig().getPort())
                            && !confirmedHosts.contains(uri.getHost() + uri.getPort())) {
                        ok = confirmServer(localClusterInfo, uri);
                        if (ok) {
                            confirmedHosts.add(uri.getHost() + uri.getPort());
                        } else {
                            break;
                        }
                    }
                }
                confirmed = ok;
                if (!ok) {
                    break;
                }
            }
            if (!confirmed) {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    LOGGER.error("sleep interrupted", e);
                }
            }
        }
        LOGGER.info("cluster confirmed");
    }

    private boolean confirmServer(ClusterInfo localClusterInfo, KVDBURI uri) {
        NettyClient client = null;
        try {
            LOGGER.info("cluster confirm {}", uri.getOrigin());
            CountDownLatch cdl = new CountDownLatch(1);
            client = new NettyClient(new ClientConfig(uri.getHost(), uri.getPort(), uri.getDatabase()), () -> cdl.countDown());
            cdl.await(CLUSTER_CONFIRM_TIME_OUT, TimeUnit.MILLISECONDS);
            Response response = client.send(KVDBMessage.clusterInfo());
            if (null == response || response.getCode() == Constants.ERROR) {
                LOGGER.error("cluster confirm {} error", BytesUtils.toString(response.getResult()[0].toBytes()));
                return false;
            }

            return localClusterInfo.match(serverContext.getConfig().getKvdbConfig().getPort(), uri, BinaryProtocol.decodeAs(response.getResult()[0].toBytes(), ClusterInfo.class));
        } catch (Exception e) {
            LOGGER.error("cluster confirm {} error", uri.getOrigin(), e);
            return false;
        } finally {
            if (null != client) {
                client.stop();
            }
        }
    }

}
