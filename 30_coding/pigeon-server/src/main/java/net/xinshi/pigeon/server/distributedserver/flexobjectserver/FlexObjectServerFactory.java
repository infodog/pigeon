package net.xinshi.pigeon.server.distributedserver.flexobjectserver;

import net.xinshi.pigeon.distributed.bean.ServerConfig;
import net.xinshi.pigeon.distributed.duplicate.DuplicateService;
import net.xinshi.pigeon.flexobject.impls.fastsimple.CommonFlexObjectFactory;
import net.xinshi.pigeon.flexobject.impls.fastsimple.OracleFlexObjectFactory;
import net.xinshi.pigeon.server.distributedserver.BaseServerFactory;

import java.io.File;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午5:10
 * To change this template use File | Settings | File Templates.
 */

public class FlexObjectServerFactory extends BaseServerFactory {

    CommonFlexObjectFactory flexFactory;
    FlexObjectServer flexObjectServer;

    public FlexObjectServerFactory(ServerConfig sc) {
        super(sc);
    }

    private CommonFlexObjectFactory createFlexObjectFactory() throws Exception {
        CommonFlexObjectFactory flexFactory = null;
        if (getSc().getDriverClass() != null && getSc().getDriverClass().startsWith("oracle")) {
            flexFactory = new OracleFlexObjectFactory();
        } else {
            flexFactory = new CommonFlexObjectFactory();
        }
        createDs();
        flexFactory.setDs(getDs());
        flexFactory.setVersionKeyName(getSc().getInstanceName());
        flexFactory.setTableName(getSc().getTable());
        if (getSc().getMaxCacheNumber() > 0) {
            flexFactory.setMaxCacheNumber(getSc().getMaxCacheNumber());
        }
        String dir = getSc().getLogDir();
        dir = dir.replace("\\", "/");
        if (!dir.endsWith("/")) {
            dir = dir + "/";
        }
        File file = new File(dir);
        if (!file.exists()) {
            file.mkdirs();
        }
        flexFactory.setLogDirectory(dir);
        flexFactory.setDs(getDs());
        flexFactory.setTxManager(getTxManager());
        flexFactory.setMaxCacheNumber(getSc().getMaxCacheNumber());
        DuplicateService.initDuplicateServer(flexFactory, getSc());
        flexFactory.init();
        this.flexFactory = flexFactory;
        return flexFactory;
    }

    public FlexObjectServer createFlexObjectServer() throws Exception {
        createFlexObjectFactory();
        flexObjectServer = new FlexObjectServer();
        flexObjectServer.setNodeName(getSc().getNodeName());
        flexObjectServer.setInstanceName(getSc().getInstanceName());
        flexObjectServer.setType(getSc().getType());
        flexObjectServer.setFlexObjectFactory(flexFactory);

        if(DuplicateService.getDuplicateServerByServerConfig(getSc())!=null){
            char role = DuplicateService.getDuplicateServerByServerConfig(getSc()).getRole();
            flexObjectServer.setRole(role);
        }
        else{
            flexObjectServer.setRole('M');
        }
        flexObjectServer.setNodesString(getSc().getNodeName());



        return flexObjectServer;
    }

}
