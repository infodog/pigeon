package net.xinshi.pigeon.server.standalongserver.atomServer;

import net.xinshi.pigeon.atom.IIntegerAtom;
import net.xinshi.pigeon.atom.impls.dbatom.FastAtom;
import net.xinshi.pigeon.server.standalongserver.BaseServerFactory;

import java.io.File;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: zhengxiangyang
 * Date: 11-10-30
 * Time: 上午3:57
 * To change this template use File | Settings | File Templates.
 */
public class AtomServerFactory extends BaseServerFactory {

    private IIntegerAtom createAtomService() throws Exception {
        FastAtom fastAtom = new FastAtom();
        ds = createDs();
        fastAtom.setDs(ds);
        fastAtom.setTableName((String) config.get("table"));
        fastAtom.setTxManager(txManager);

        //TODU Those args need to get from ConfigServer
        fastAtom.setFastCreate(true);
        fastAtom.setVersionKeyName((String) config.get("instanceName"));

        String dir = (String) config.get("logDir");
        if (!dir.endsWith("/")) {
            dir += "/";
        }
        File file = new File(dir);
        if (!file.exists()) {
            file.mkdirs();
        }
        fastAtom.setLogDirectory(dir);

        fastAtom.init();

        return fastAtom;
    }


    public AtomServer createAtomServer(Map config) throws Exception {
        this.config = config;
        IIntegerAtom atom = this.createAtomService();
        AtomServer atomServer = new AtomServer();


        boolean isMaster = (Boolean) config.get("master");
        atomServer.setMaster(isMaster);
        String seedUrl = (String) config.get("seedUrl");
        atomServer.setSeedUrl(seedUrl);
        String localPort = (String) config.get("localPort");
        atomServer.setLocalPort(localPort);
        String nodeName = (String) config.get("nodeName");
        atomServer.setNodeName(nodeName);
        String instanceName = (String) config.get("instanceName");
        atomServer.setInstanceName(instanceName);
        atomServer.setType((String) config.get("type"));


        atomServer.setAtom(atom);

        return atomServer;
    }


}
