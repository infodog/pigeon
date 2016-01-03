package net.xinshi.pigeon.server.standalongserver.listServer;

import net.xinshi.pigeon.list.IListFactory;
import net.xinshi.pigeon.list.bandlist.ListBandDao;
import net.xinshi.pigeon.list.bandlist.SortBandListFactory;
import net.xinshi.pigeon.list.bandlist.SortBandStringSerializer;
import net.xinshi.pigeon.server.standalongserver.BaseServerFactory;

import java.io.File;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: zhengxiangyang
 * Date: 11-10-30
 * Time: 上午3:10
 * To change this template use File | Settings | File Templates.
 */
public class ListServerFactory extends BaseServerFactory {


    private IListFactory createListFactory() throws Exception {
        SortBandListFactory listFactory = new SortBandListFactory();
        ds = createDs();
        listFactory.setDs(ds);
        listFactory.setVersionKeyName((String) config.get("instanceName"));
        listFactory.setIdDataSource(ds);
        listFactory.setIdTxManager(this.txManager);
        listFactory.setTxManager(txManager);
        listFactory.setFactoryName("pigeonDefaultList");

        ListBandDao dao = new ListBandDao();
        dao.setDs(ds);
        dao.setTableName((String) config.get("table"));
        listFactory.setDao(dao);
        String dir = (String) config.get("logDir");
        if (!dir.endsWith("/")) {
            dir += "/";
        }
        File file = new File(dir);
        if (!file.exists()) {
            file.mkdirs();
        }
        listFactory.setLogDirectory(dir);

        //TODO get the value from ConfigServer

        listFactory.setMaxBandCacheSize(30000);
        listFactory.setMaxListCacheSize(5000);

        listFactory.setMaxBandInfosPerBand(500);
        listFactory.setMaxObjectsPerBand(500);

        SortBandStringSerializer serializer = new SortBandStringSerializer();
        listFactory.setBandSerializer(serializer);

        listFactory.init();

        return listFactory;
    }


    public ListServer createListServer(Map config) throws Exception {
        this.config = config;
        IListFactory factory = createListFactory();
        ListServer listServer = new ListServer();

        boolean isMaster = (Boolean) config.get("master");
        listServer.setMaster(isMaster);
        String seedUrl = (String) config.get("seedUrl");
        listServer.setSeedUrl(seedUrl);
        String localPort = (String) config.get("localPort");
        listServer.setLocalPort(localPort);
        String nodeName = (String) config.get("nodeName");
        listServer.setNodeName(nodeName);
        String instanceName = (String) config.get("instanceName");
        listServer.setInstanceName(instanceName);
        listServer.setType((String) config.get("type"));


        listServer.setFactory(factory);


        return listServer;
    }
}
