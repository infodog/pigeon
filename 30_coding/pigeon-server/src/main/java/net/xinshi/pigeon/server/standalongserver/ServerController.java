package net.xinshi.pigeon.server.standalongserver;

import net.xinshi.pigeon.server.standalongserver.atomServer.AtomServer;
import net.xinshi.pigeon.server.standalongserver.atomServer.AtomServerFactory;
import net.xinshi.pigeon.server.standalongserver.atomServer.HttpAtomServerConnector;
import net.xinshi.pigeon.server.standalongserver.fileserver.FileServer;
import net.xinshi.pigeon.server.standalongserver.fileserver.FileServerConnector;
import net.xinshi.pigeon.server.standalongserver.fileserver.FileServerFactory;
import net.xinshi.pigeon.server.standalongserver.flexobjectServer.FlexObjectServer;
import net.xinshi.pigeon.server.standalongserver.flexobjectServer.FlexObjectServerFactory;
import net.xinshi.pigeon.server.standalongserver.flexobjectServer.HttpFlexObjectServerConnector;
import net.xinshi.pigeon.server.standalongserver.idserver.HttpIdServerConnector;
import net.xinshi.pigeon.server.standalongserver.idserver.IdServer;
import net.xinshi.pigeon.server.standalongserver.idserver.IdServerFactory;
import net.xinshi.pigeon.server.standalongserver.listServer.HttpListServerConnector;
import net.xinshi.pigeon.server.standalongserver.listServer.ListServer;
import net.xinshi.pigeon.server.standalongserver.listServer.ListServerFactory;
import net.xinshi.pigeon.server.standalongserver.lockServer.MinaResourceLockServer;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.params.ConnManagerParams;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpRequestHandlerRegistry;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Created by IntelliJ IDEA.
 * User: zxy
 * Date: 2010-7-31
 * Time: 17:35:14
 * To change this template use File | Settings | File Templates.
 */

public class ServerController {

    Map<String, Object> servers;
    Logger log = Logger.getLogger(ServerController.class.getName());

    String[] configServers;

    HttpClient httpClient;

    String host;

    public ServerController() {
        servers = new Hashtable<String, Object>();
    }

    synchronized public void setMaster(String serverName) throws Exception {
        BaseServer server = (BaseServer) servers.get(serverName);
        server.setMaster(true);
    }

    public void shutDown(String serverName) throws Exception {
        //DO thing

    }

    synchronized public IServer start(Map config, HttpRequestHandlerRegistry registry) throws Exception {
        String instanceName = (String) config.get("instanceName");
        String nodeName = (String) config.get("nodeName");
        if (StringUtils.isBlank(nodeName)) {
            log.log(Level.SEVERE
                    , "serverName can not be null.");
            return null;
        }
        if (StringUtils.isBlank(instanceName)) {
            log.log(Level.SEVERE, "instanceName can not be null.");
            return null;
        }
        if (servers.containsKey(instanceName)) {
            throw new Exception(instanceName + ": Server already exists.");
        }
        this.host = (String) config.get("host");
        String type = (String) config.get("type");

        IServer server = null;
        if ("obj".equals(type)) {
            FlexObjectServerFactory factory = new FlexObjectServerFactory();
            server = factory.createFlexObjectServer(config);
            servers.put(instanceName, server);
            HttpFlexObjectServerConnector connector = new HttpFlexObjectServerConnector();
            connector.setFlexObjectHandler((FlexObjectServer) server);
            registry.register(instanceName, connector);
            System.out.println("started flexobject server of instance name = " + instanceName + " ...");
        } else if ("list".equals(type)) {
            ListServerFactory factory = new ListServerFactory();
            server = factory.createListServer(config);
            servers.put(instanceName, server);
            HttpListServerConnector connector = new HttpListServerConnector();
            connector.setListServer((ListServer) server);
            registry.register(instanceName, connector);
            System.out.println("started list server of instance name = " + instanceName + " ...");
        } else if ("atom".equals(type)) {
            AtomServerFactory factory = new AtomServerFactory();
            server = factory.createAtomServer(config);
            servers.put(instanceName, server);
            HttpAtomServerConnector connector = new HttpAtomServerConnector();
            connector.setAtomServer((AtomServer) server);
            registry.register(instanceName, connector);
            System.out.println("started atom of instance name = " + instanceName + " ...");
        } else if ("lock".equals(type)) {
            MinaResourceLockServer lockServer = new MinaResourceLockServer();
            servers.put(instanceName, lockServer);
            server = null;
            lockServer.start(config);
            System.out.println("started lock  ...");
        }
        else if ("idserver".equals(type)){
            IdServerFactory idServerFactory = new IdServerFactory();
            server = idServerFactory.createIdServer(config);
            servers.put(instanceName, server);
            HttpIdServerConnector idServerConnector = new  HttpIdServerConnector();
            idServerConnector.setIdServer((IdServer)server);
            registry.register(instanceName,idServerConnector);
        }
        else if("fileserver".equals(type)){
            FileServerFactory fileServerFactory = new FileServerFactory();
            FileServer fileServer = fileServerFactory.createFileServer(config);
            server = fileServer;
            servers.put(instanceName, server);
            FileServerConnector connector = new FileServerConnector();
            connector.setFileServer(fileServer);
            registry.register(instanceName,connector);
        }

        return server;
    }

    public void startServers(List<Map> serverConfigs, HttpRequestHandlerRegistry registry) throws Exception {
        for (Map config : serverConfigs) {
            try {
                start(config, registry);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void setConfigServer(String[] configServers) throws Exception {
        this.configServers = configServers;
    }

    public void initHttpClient() {

        // Create and initialize HTTP parameters
        HttpParams params = new BasicHttpParams();
        ConnManagerParams.setMaxTotalConnections(params, 100);
        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);

        // Create and initialize scheme registry
        SchemeRegistry schemeRegistry = new SchemeRegistry();
        schemeRegistry.register(new Scheme("http", PlainSocketFactory.getSocketFactory(), 80));

        // Create an HttpClient with the ThreadSafeClientConnManager.
        // This connection manager must be used if more than one thread will
        // be using the HttpClient.
        ClientConnectionManager cm = new ThreadSafeClientConnManager(params, schemeRegistry);
        httpClient = new DefaultHttpClient(cm, params);
    }

    List<Map> getFileConfigs() throws Exception {
        for (int i = 0; i < this.configServers.length; i = i + 1) {
            try {
                String url = configServers[i];
                File f = new File(url);

                FileInputStream is = new FileInputStream(url);
                byte[] b = new byte[(int) f.length()];
                is.read(b);

                is.close();

                String s = new String(b, "UTF-8");
                s = StringUtils.trim(s);
                JSONObject jo = new JSONObject(s);
                JSONArray jconfigs = jo.getJSONArray("pigeons");

                List configs = new Vector();
                for (int j = 0; j < jconfigs.length(); j++) {
                    JSONObject jconfig = jconfigs.getJSONObject(j);
                    jconfig.put("seedUrl", url);
                    configs.add(jconfig.getObjectMap());
                }
                return configs;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        throw new Exception("no config server available");
    }

    List<Map> downLoadConfigs() throws Exception {
        initHttpClient();
        for (int i = 0; i < this.configServers.length; i = i + 1) {
            try {
                String url = configServers[i];
                HttpPost httpGet = new HttpPost(url);


                List<NameValuePair> nvps = new ArrayList<NameValuePair>();
                httpGet.setEntity(new UrlEncodedFormEntity(nvps, HTTP.UTF_8));

                HttpResponse response = httpClient.execute(httpGet);
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    String s = EntityUtils.toString(entity, "UTF-8");
                    s = StringUtils.trim(s);
                    JSONObject jo = new JSONObject(s);
                    JSONArray jconfigs = jo.getJSONArray("pigeons");
                    System.out.println(jconfigs);
                    List configs = new Vector();
                    for (int j = 0; j < jconfigs.length(); j++) {
                        JSONObject jconfig = jconfigs.getJSONObject(j);
                        jconfig.put("seedUrl", url);
                        configs.add(jconfig.getObjectMap());
                    }
                    return configs;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        throw new Exception("no config server available");
    }


    List<Map> getConfigs() throws Exception {
        String url = this.configServers[0];
        String upperCase = StringUtils.upperCase(url);
        if (upperCase.startsWith("HTTP:")) {
            return downLoadConfigs();
        } else {
            return getFileConfigs();
        }
    }

    public void start(HttpRequestHandlerRegistry registry) throws Exception {


        //1.下载configs
        List<Map> configs = null;

        try {
            configs = getConfigs();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw e;
        }


        startServers(configs, registry);
    }

    public Map<String, Object> getServers() {
        return servers;
    }

    public String getHost() {
        return host;
    }
}
