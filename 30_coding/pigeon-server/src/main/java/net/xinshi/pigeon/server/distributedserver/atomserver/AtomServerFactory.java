package net.xinshi.pigeon.server.distributedserver.atomserver;

import net.xinshi.pigeon.atom.IIntegerAtom;
import net.xinshi.pigeon.atom.impls.dbatom.FastAtom;
import net.xinshi.pigeon.distributed.bean.ServerConfig;
import net.xinshi.pigeon.distributed.duplicate.DuplicateService;
import net.xinshi.pigeon.server.distributedserver.BaseServerFactory;

import java.io.File;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午3:58
 * To change this template use File | Settings | File Templates.
 */

public class AtomServerFactory extends BaseServerFactory {

    public AtomServerFactory(ServerConfig sc) {
        super(sc);
    }

    private IIntegerAtom createAtomService() throws Exception {
        FastAtom fastAtom = new FastAtom();
        createDs();
        fastAtom.setServerConfig(getSc());
        fastAtom.setDs(getDs());
        fastAtom.setTableName(getSc().getTable());
        fastAtom.setTxManager(getTxManager());
        fastAtom.setFastCreate(true);
        fastAtom.setVersionKeyName(getSc().getInstanceName());
        if (getSc().getMaxCacheNumber() > 0) {
            fastAtom.setMaxCacheEntries(getSc().getMaxCacheNumber());
        }
        String dir = getSc().getLogDir();
        dir = dir.replace("\\", "/");
        if (!dir.endsWith("/")) {
            dir += "/";
        }
        File file = new File(dir);
        if (!file.exists()) {
            file.mkdirs();
        }
        fastAtom.setLogDirectory(dir);
        DuplicateService.initDuplicateServer(fastAtom, getSc());
        fastAtom.init();
        return fastAtom;
    }

    public AtomServer createAtomServer() throws Exception {
        IIntegerAtom atom = this.createAtomService();
        AtomServer atomServer = new AtomServer();
        atomServer.setNodeName(getSc().getNodeName());
        atomServer.setInstanceName(getSc().getInstanceName());
        atomServer.setType(getSc().getType());
        atomServer.setAtom(atom);
        if(DuplicateService.getDuplicateServerByServerConfig(getSc())!=null){
            char role = DuplicateService.getDuplicateServerByServerConfig(getSc()).getRole();
            atomServer.setRole(role);
        }
        else{
            atomServer.setRole('M');
        }
        atomServer.setNodesString(getSc().getNodeName());


        return atomServer;
    }

}

