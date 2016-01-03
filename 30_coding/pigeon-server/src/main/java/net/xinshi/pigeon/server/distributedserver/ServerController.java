package net.xinshi.pigeon.server.distributedserver;

import net.xinshi.pigeon.distributed.bean.ServerConfig;
import net.xinshi.pigeon.distributed.duplicate.DuplicateConfig;
import net.xinshi.pigeon.distributed.duplicate.DuplicateService;
import net.xinshi.pigeon.server.distributedserver.atomserver.AtomServer;
import net.xinshi.pigeon.server.distributedserver.atomserver.AtomServerFactory;
import net.xinshi.pigeon.server.distributedserver.atomserver.HttpAtomServerConnector;
import net.xinshi.pigeon.server.distributedserver.fileserver.FileServer;
import net.xinshi.pigeon.server.distributedserver.fileserver.FileServerConnector;
import net.xinshi.pigeon.server.distributedserver.fileserver.FileServerFactory;
import net.xinshi.pigeon.server.distributedserver.flexobjectserver.FlexObjectServer;
import net.xinshi.pigeon.server.distributedserver.flexobjectserver.FlexObjectServerFactory;
import net.xinshi.pigeon.server.distributedserver.flexobjectserver.HttpFlexObjectServerConnector;
import net.xinshi.pigeon.server.distributedserver.idserver.HttpIdServerConnector;
import net.xinshi.pigeon.server.distributedserver.idserver.IdServer;
import net.xinshi.pigeon.server.distributedserver.idserver.IdServerFactory;
import net.xinshi.pigeon.server.distributedserver.listserver.HttpListServerConnector;
import net.xinshi.pigeon.server.distributedserver.listserver.ListServer;
import net.xinshi.pigeon.server.distributedserver.listserver.ListServerFactory;
import net.xinshi.pigeon.server.distributedserver.lockserver.NettyLockServerHandler;
import org.apache.commons.lang.StringUtils;
import org.apache.http.protocol.HttpRequestHandlerRegistry;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午2:32
 * To change this template use File | Settings | File Templates.
 */

public class ServerController {

    List<ServerConfig> listConfigs;
    Map<String, Object> servers;
    Logger log = Logger.getLogger(ServerController.class.getName());
    String configfile = null;
    Logger logger = Logger.getLogger(ServerController.class.getName());

    public ServerController(String configfile) {
        this.configfile = configfile;
        servers = new Hashtable<String, Object>();
    }

    synchronized public IServer start(ServerConfig sc, HttpRequestHandlerRegistry registry) throws Exception {
        String instanceName = sc.getInstanceName();
        String nodeName = sc.getNodeName();
        if (StringUtils.isBlank(nodeName)) {
            log.log(Level.SEVERE, "serverName can not be null.");
            return null;
        }
        if (StringUtils.isBlank(instanceName)) {
            log.log(Level.SEVERE, "instanceName can not be null.");
            return null;
        }
        if (servers.containsKey(instanceName)) {
            throw new Exception(instanceName + ": Server already exists.");
        }
        String type = sc.getType();
        IServer server = null;
        if ("flexobject".equals(type)) {
            FlexObjectServerFactory factory = new FlexObjectServerFactory(sc);
            server = factory.createFlexObjectServer();
            servers.put(instanceName, server);
            HttpFlexObjectServerConnector connector = new HttpFlexObjectServerConnector();
            connector.setFlexObjectHandler((FlexObjectServer) server);
            registry.register(instanceName, connector);
            System.out.println("started flexobject server of instance name = " + instanceName + " ...");
        } else if ("list".equals(type)) {
            ListServerFactory factory = new ListServerFactory(sc);
            server = factory.createListServer();
            servers.put(instanceName, server);
            HttpListServerConnector connector = new HttpListServerConnector();
            connector.setListServer((ListServer) server);
            registry.register(instanceName, connector);
            System.out.println("started list server of instance name = " + instanceName + " ...");
        } else if ("atom".equals(type)) {
            AtomServerFactory factory = new AtomServerFactory(sc);
            server = factory.createAtomServer();
            servers.put(instanceName, server);
            HttpAtomServerConnector connector = new HttpAtomServerConnector();
            connector.setAtomServer((AtomServer) server);
            registry.register(instanceName, connector);
            System.out.println("started atom of instance name = " + instanceName + " ...");
        } else if ("lock".equals(type)) {
            NettyLockServerHandler lockServer = new NettyLockServerHandler(sc);
            servers.put(instanceName, lockServer);
            server = null;
            lockServer.init();
            System.out.println("started lock  ...");
        } else if ("idserver".equals(type)) {
            IdServerFactory idServerFactory = new IdServerFactory(sc);
            server = idServerFactory.createIdServer();
            servers.put(instanceName, server);
            HttpIdServerConnector idServerConnector = new HttpIdServerConnector();
            idServerConnector.setIdServer((IdServer) server);
            registry.register(instanceName, idServerConnector);
        } else if ("fileserver".equals(type)) {
            FileServerFactory fileServerFactory = new FileServerFactory(sc);
            FileServer fileServer = fileServerFactory.createFileServer();
            server = fileServer;
            servers.put(instanceName, server);
            FileServerConnector connector = new FileServerConnector();
            connector.setFileServer(fileServer);
            registry.register(instanceName, connector);
        }
        return server;
    }

    public void startServers(HttpRequestHandlerRegistry registry) throws Exception {
        for (ServerConfig sc : listConfigs) {
            try {
                start(sc, registry);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    List<ServerConfig> createServerConfigs(String s) throws Exception {
        s = StringUtils.trim(s);
        JSONObject jo = new JSONObject(s);
        JSONArray jconfigs = jo.getJSONArray("pigeons");
        List<ServerConfig> listSC = new ArrayList<ServerConfig>();
        for (int j = 0; j < jconfigs.length(); j++) {
            JSONObject jconfig = jconfigs.getJSONObject(j);
            ServerConfig sc = ServerConfig.JSONObject2ServerConfig(jconfig);
            listSC.add(sc);
        }
        return listSC;
    }

    public void init() throws Exception {
        if (configfile != null) {
            File f = new File(configfile);
            FileInputStream is = new FileInputStream(f);
            byte[] b = new byte[(int) f.length()];
            is.read(b);
            is.close();
            String s = new String(b, "UTF-8");
            listConfigs = createServerConfigs(s);
            return;
        }
        throw new Exception("ServerController init error ...... ");
    }

    public void initDuplicateController(String controlFile) throws Exception {

        if (controlFile != null) {
            File f = new File(controlFile);
            if (!f.exists() || f.length() == 0) {

                logger.info("initDuplicateController controlFile=" + controlFile + "not found.");
                controlFile = null;
                return;
            }
            DuplicateConfig duplicateConfig = new DuplicateConfig(controlFile);
            duplicateConfig.init();
            DuplicateService.setDuplicateConfig(duplicateConfig);
        }
    }

    public void start(HttpRequestHandlerRegistry registry) throws Exception {
        startServers(registry);
    }

    public Map<String, Object> getServers() {
        return servers;
    }

    public boolean set_servers_state_word(int state_word) {
        try {
            for (Object obj : servers.values()) {
                if (obj instanceof FlexObjectServer) {
                    ((FlexObjectServer) obj).set_state_word(state_word);
                } else if (obj instanceof ListServer) {
                    ((ListServer) obj).getFactory().set_state_word(state_word);
                } else if (obj instanceof AtomServer) {
                    ((AtomServer) obj).getAtom().set_state_word(state_word);
                } else if (obj instanceof IdServer) {
                    ((IdServer) obj).getIdgenerator().set_state_word(state_word);
                }
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

}

