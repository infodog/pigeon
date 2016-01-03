package net.xinshi.pigeon.server.standalongserver.fileserver;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.adapter.impl.NettySingleServerEngine;
import net.xinshi.pigeon.filesystem.impl.PigeonFileSystemConfig;
import net.xinshi.pigeon.server.standalongserver.BaseServerFactory;

import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: mac
 * Date: 11-12-1
 * Time: 下午3:49
 * To change this template use File | Settings | File Templates.
 */

public class FileServerFactory extends BaseServerFactory {

    FileServer fileServer;
    IPigeonStoreEngine pigeonStore;

    public FileServer createFileServer(Map config) throws Exception {
        this.config = config;
        fileServer = new FileServer();
        String baseDir = (String) config.get("baseDir");
        fileServer.setBaseDir(baseDir);
        pigeonStore = new NettySingleServerEngine((String) config.get("pigeonStoreConfigFile"));
        fileServer.setPigeonStoreEngine(pigeonStore);
        fileServer.setName((String) config.get("name"));
        PigeonFileSystemConfig pigeonFileSystemConfig = new PigeonFileSystemConfig((String) config.get("pigeonFileSystemConfigFile"));
        fileServer.setPigeonFileSystemConfig(pigeonFileSystemConfig);
        fileServer.init();
        return fileServer;
    }
}
