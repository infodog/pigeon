package net.xinshi.pigeon.server.standalongserver.idserver;

import net.xinshi.pigeon.idgenerator.impl.MysqlIDGenerator;
import net.xinshi.pigeon.server.standalongserver.BaseServerFactory;

import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: mac
 * Date: 11-11-12
 * Time: 下午5:28
 * To change this template use File | Settings | File Templates.
 */
public class IdServerFactory extends BaseServerFactory {
    public IdServer createIdServer(Map config) throws Exception {
        this.config = config;
        this.createDs();

        MysqlIDGenerator idGenerator = new MysqlIDGenerator();
        idGenerator.setDs(ds);
        IdServer idServer = new IdServer();
        idServer.setIdgenerator(idGenerator);
        return idServer;

    }
}
