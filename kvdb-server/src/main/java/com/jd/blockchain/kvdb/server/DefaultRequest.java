package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.kvdb.protocol.Command;
import com.jd.blockchain.kvdb.protocol.Message;

public class DefaultRequest implements Request {

    private final ServerContext server;
    private final Session session;
    private final Message message;

    public DefaultRequest(ServerContext server, Session session, Message message) {
        this.server = server;
        this.session = session;
        this.message = message;
    }

    @Override
    public String getId() {
        return message.getId();
    }

    @Override
    public Command getCommand() {
        return (Command) message.getContent();
    }

    @Override
    public Session getSession() {
        return session;
    }

    @Override
    public ServerContext getServerContext() {
        return server;
    }

}
