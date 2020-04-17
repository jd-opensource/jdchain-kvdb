package com.jd.blockchain.kvdb.cli;

import com.jd.blockchain.kvdb.protocol.client.ClientConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class CliBooter {

    private static final int DEFAULT_PORT = 7060;
    /**
     * 保存 kvdb-cli 启动参数，参数列表参照{@link ClientConfig}
     */
    private static String[] clientArgs;

    public static void main(String[] args) {
        clientArgs = args;
        SpringApplication.run(CliBooter.class);
    }

    @Bean
    public ClientConfig clientConfig() {
        ClientConfig config = new ClientConfig(clientArgs);
        boolean containPort = false;
        for (String arg : clientArgs) {
            if (arg.equals("-p")) {
                containPort = true;
                break;
            }
        }
        if (!containPort) {
            config.setPort(DEFAULT_PORT);
        }
        return config;
    }

}
