package net.xinshi.pigeon.server.distributedserver.listserver;

import net.xinshi.pigeon.distributed.bean.ServerConfig;
import net.xinshi.pigeon.distributed.duplicate.DuplicateService;
import net.xinshi.pigeon.list.IListFactory;
import net.xinshi.pigeon.list.bandlist.ListBandDao;
import net.xinshi.pigeon.list.bandlist.SortBandListFactory;
import net.xinshi.pigeon.list.bandlist.SortBandStringSerializer;
import net.xinshi.pigeon.server.distributedserver.BaseServerFactory;

import java.io.File;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午5:32
 * To change this template use File | Settings | File Templates.
 */

public class ListServerFactory extends BaseServerFactory {

    public ListServerFactory(ServerConfig sc) {
        super(sc);
    }

    private IListFactory createListFactory() throws Exception {
        SortBandListFactory listFactory = new SortBandListFactory();
        createDs();
        listFactory.setServerConfig(getSc());
        listFactory.setDs(getDs());
        listFactory.setVersionKeyName(getSc().getInstanceName());
        listFactory.setIdDataSource(getDs());
        listFactory.setIdTxManager(getTxManager());
        listFactory.setTxManager(getTxManager());
        listFactory.setFactoryName("pigeonDefaultList");
        listFactory.setMigration(getSc().isMigration());
        if (getSc().getMaxCacheNumber() > 0) {
            listFactory.setMaxListCacheSize(getSc().getMaxCacheNumber());
        }
        ListBandDao dao = new ListBandDao();
        dao.setDs(getDs());
        dao.setTableName(getSc().getTable());
        listFactory.setDao(dao);
        String dir = getSc().getLogDir();
        dir = dir.replace("\\", "/");
        if (!dir.endsWith("/")) {
            dir += "/";
        }
        File file = new File(dir);
        if (!file.exists()) {
            file.mkdirs();
        }
        listFactory.setLogDirectory(dir);
        listFactory.setMaxBandCacheSize(30000);
        // listFactory.setMaxListCacheSize(5000);
        listFactory.setMaxBandInfosPerBand(500);
        listFactory.setMaxObjectsPerBand(500);
        SortBandStringSerializer serializer = new SortBandStringSerializer();
        listFactory.setBandSerializer(serializer);
        DuplicateService.initDuplicateServer(listFactory, getSc());
        listFactory.init();
        return listFactory;
    }

    public ListServer createListServer() throws Exception {
        IListFactory factory = createListFactory();
        ListServer listServer = new ListServer();
        listServer.setNodeName(getSc().getNodeName());
        listServer.setInstanceName(getSc().getInstanceName());
        listServer.setType(getSc().getType());
        listServer.setFactory(factory);
        if(DuplicateService.getDuplicateServerByServerConfig(getSc())!=null){
            char role = DuplicateService.getDuplicateServerByServerConfig(getSc()).getRole();
            listServer.setRole(role);
        }
        else{
            listServer.setRole('M');
        }
        listServer.setNodesString(getSc().getNodeName());
        return listServer;
    }
}

