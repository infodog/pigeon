package net.xinshi.pigeon.pigeonserver;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.adapter.impl.DistributedPigeonEngine;
import net.xinshi.pigeon.filesystem.impl.PigeonFileSystem;
import net.xinshi.pigeon.server.distributedserver.PigeonServer;
import net.xinshi.pigeon.server.standalongserver.Constants;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-1-29
 * Time: 上午10:50
 * To change this template use File | Settings | File Templates.
 */
public class ServerDeamon {


    static IPigeonStoreEngine pigeonStoreEngine;
    static PigeonFileSystem fileSystem;
    static Logger logger = Logger.getLogger("pigeonServerTest");

    public static void main(String[] args) throws Exception {
        logger.log(Level.INFO, "setup");
        startServers();
        //createPigeonClient();
        //createPigeonFileSystem();
    }

    public static void startServers() throws Exception {
        File f = new File("pigeon-server/src/test/resources/pigeonservertest.conf");
        /*  PigeonListener.startServers(Integer.parseInt(Constants.defaultPort),
       Integer.parseInt(Constants.defaultNettyPort),
       new String[]{f.getAbsolutePath()});*/
        // PigeonServer.controlFile = "F:\\PigeonServerBackup\\conf/pigeonnodes.conf";
        PigeonServer.startServers(Integer.parseInt(Constants.defaultPort),
                Integer.parseInt(Constants.defaultNettyPort),
                f.getAbsolutePath());
    }


    public static void createPigeonClient() throws Exception {
        File f = new File("pigeon-server/src/test/resources/pigeonnodes.conf");
        pigeonStoreEngine = new DistributedPigeonEngine(f.getAbsolutePath());
    }

    public static void createPigeonFileSystem() throws Exception {
        fileSystem = new PigeonFileSystem(pigeonStoreEngine, "pigeon-server/src/test/resources/pigeonfilesystem.conf");
    }

}

