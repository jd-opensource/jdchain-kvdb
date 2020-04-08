package com.jd.blockchain.kvdb.client.cli;

import com.jd.blockchain.kvdb.client.KVDBClient;
import com.jd.blockchain.kvdb.protocol.client.ClientConfig;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class CliBooter {

    private static String[] clientArgs;

    public static void main(String[] args) {
        clientArgs = args;
        SpringApplication.run(CliBooter.class);
    }

    @Bean
    public ClientConfig clientConfig() {
        return new ClientConfig(clientArgs);
    }

    @Bean
    @Autowired
    public KVDBClient client(ClientConfig clientConfig) throws KVDBException {
        return new KVDBClient(clientConfig);
    }

}
