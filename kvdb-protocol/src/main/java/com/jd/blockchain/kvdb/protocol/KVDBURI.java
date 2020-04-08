package com.jd.blockchain.kvdb.protocol;

import java.net.*;

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
     * Localhost ip check
     *
     * @param host
     * @return
     */
    public static boolean isLocalhost(String host) {
        try {
            return isLocalhost(InetAddress.getByName(host));
        } catch (UnknownHostException e) {
            return false;
        }
    }

    private static boolean isLocalhost(InetAddress addr) {
        if (addr.isAnyLocalAddress() || addr.isLoopbackAddress())
            return true;

        try {
            return NetworkInterface.getByInetAddress(addr) != null;
        } catch (SocketException e) {
            return false;
        }
    }
}
