package com.jd.blockchain.kvdb.protocol;

import com.jd.blockchain.utils.StringUtils;

import java.net.InetAddress;
import java.net.NetworkInterface;

public class URIUtils {

    /**
     * 是否本机地址
     *
     * @param host
     * @return
     */
    public static boolean isLocalhost(String host) {
        try {
            InetAddress addr = InetAddress.getByName(host);
            if (addr.isAnyLocalAddress() || addr.isLoopbackAddress())
                return true;
            return NetworkInterface.getByInetAddress(addr) != null;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 是否指向相同服务器
     *
     * @param h1
     * @param h2
     * @return
     */
    public static boolean isSameHost(String h1, String h2) {
        try {
            if (StringUtils.isEmpty(h1) || StringUtils.isEmpty(h2)) {
                return false;
            }
            if (h1.equals(h2)) {
                return true;
            }
            if(isLocalhost(h1) && isLocalhost(h2)) {
                return true;
            }
            String host1 = InetAddress.getByName(h1).getHostAddress();
            String host2 = InetAddress.getByName(h2).getHostAddress();
            return host1.equals(host2);
        } catch (Exception e) {
            return false;
        }
    }
}
