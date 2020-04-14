package com.jd.blockchain.kvdb.protocol;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URI;

public class KVDBURI {

    private String origin;
    private URI uri;

    public KVDBURI(String origin) {
        this.origin = origin;
        this.uri = URI.create(origin);
    }

    public String getOrigin() {
        return origin;
    }

    public String getHost() {
        return uri.getHost();
    }

    public int getPort() {
        return uri.getPort();
    }

    /**
     * Get database from URI
     *
     * @return
     */
    public String getDatabase() {
        return uri.getPath().replace("/", "");
    }

    /**
     * 是否是本地地址
     * TODO 待验证
     *
     * @return
     */
    public boolean isLocalhost() {
        try {
            InetAddress addr = InetAddress.getByName(getHost());
            if (addr.isAnyLocalAddress() || addr.isLoopbackAddress())
                return true;
            return NetworkInterface.getByInetAddress(addr) != null;
        } catch (Exception e) {
            return false;
        }

    }
}
