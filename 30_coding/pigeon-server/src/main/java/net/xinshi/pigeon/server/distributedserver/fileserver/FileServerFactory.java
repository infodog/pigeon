package net.xinshi.pigeon.server.distributedserver.fileserver;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.adapter.impl.DistributedPigeonEngine;
import net.xinshi.pigeon.distributed.bean.ServerConfig;
import net.xinshi.pigeon.filesystem.impl.PigeonFileSystemConfig;
import net.xinshi.pigeon.server.distributedserver.BaseServerFactory;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午4:31
 * To change this template use File | Settings | File Templates.
 */

public class FileServerFactory extends BaseServerFactory {

    FileServer fileServer;
    IPigeonStoreEngine pigeonStore;
    PigeonFileSystemConfig pigeonFileSystemConfig;

    public FileServerFactory(ServerConfig sc) {
        super(sc);
    }

    public FileServer createFileServer() throws Exception {
        fileServer = new FileServer(getSc());
        fileServer.setBaseDir(getSc().getBaseDir());
        pigeonStore = new DistributedPigeonEngine(getSc().getPigeonStoreConfigFile());
        fileServer.setPigeonStoreEngine(pigeonStore);
        pigeonFileSystemConfig = new PigeonFileSystemConfig(getSc().getPigeonFileSystemConfigFile());
        fileServer.setPigeonFileSystemConfig(pigeonFileSystemConfig);
        fileServer.init();
        return fileServer;
    }
}

