package net.xinshi.pigeon.pigeonserver;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.adapter.impl.DistributedPigeonEngine;
import net.xinshi.pigeon.filesystem.impl.PigeonFileSystem;
import net.xinshi.pigeon.server.distributedserver.PigeonServer;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-20
 * Time: 下午5:26
 * To change this template use File | Settings | File Templates.
 */
public class ServerDeamon2 {


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
        File f = new File("pigeon-server/src/test/resources/pigeonservertest3.conf");
        /*  PigeonListener.startServers(Integer.parseInt(Constants.defaultPort),
       Integer.parseInt(Constants.defaultNettyPort),
       new String[]{f.getAbsolutePath()});*/
        PigeonServer.controlFile = "pigeon-server/src/test/resources/pigeonnodes3.conf";
        PigeonServer.startServers(8879,
                8878,
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


