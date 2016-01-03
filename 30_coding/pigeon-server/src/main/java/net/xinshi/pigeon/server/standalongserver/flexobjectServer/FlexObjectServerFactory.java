package net.xinshi.pigeon.server.standalongserver.flexobjectServer;

import net.xinshi.pigeon.flexobject.impls.fastsimple.SimpleFlexObjectFactory;
import net.xinshi.pigeon.server.standalongserver.BaseServerFactory;

import java.io.File;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: zhengxiangyang
 * Date: 11-10-30
 * Time: 上午1:19
 * To change this template use File | Settings | File Templates.
 */
public class FlexObjectServerFactory extends BaseServerFactory {

    SimpleFlexObjectFactory flexFactory;
    FlexObjectServer flexObjectServer;


    private SimpleFlexObjectFactory createFlexObjectFactory() throws Exception {
        SimpleFlexObjectFactory flexFactory = new SimpleFlexObjectFactory();
        ds = createDs();
        flexFactory.setDs(ds);
        flexFactory.setVersionKeyName((String) config.get("instanceName"));
        flexFactory.setTableName((String) config.get("table"));
        String dir = (String) config.get("logDir");
        if (!dir.endsWith("/")) {
            dir = dir + "/";
        }
        File file = new File(dir);
        if (!file.exists()) {
            file.mkdirs();
        }
        flexFactory.setLogDirectory(dir);
        flexFactory.setDs(ds);
        flexFactory.setTxManager(txManager);
        int maxCacheNum = (Integer) config.get("maxCacheNumber");
        flexFactory.setMaxCacheNumber((int) maxCacheNum);
        String instanceName = (String) config.get("instanceName");
        flexFactory.init();
        this.flexFactory = flexFactory;
        return flexFactory;
    }


    public FlexObjectServer createFlexObjectServer(Map config) throws Exception {
        this.config = config;
        createFlexObjectFactory();
        flexObjectServer = new FlexObjectServer();

        boolean isMaster = (Boolean) config.get("master");
        flexObjectServer.setMaster(isMaster);
        String seedUrl = (String) config.get("seedUrl");
        flexObjectServer.setSeedUrl(seedUrl);
        String localPort = (String) config.get("localPort");
        flexObjectServer.setLocalPort(localPort);
        String nodeName = (String) config.get("nodeName");
        flexObjectServer.setNodeName(nodeName);
        String instanceName = (String) config.get("instanceName");
        flexObjectServer.setInstanceName(instanceName);
        flexObjectServer.setType((String) config.get("type"));
        flexObjectServer.setVersion((Integer) config.get("version"));
        flexObjectServer.setFlexObjectFactory(flexFactory);
        return flexObjectServer;
    }
}