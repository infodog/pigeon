package net.xinshi.pigeon.filesystem.impl;

import net.xinshi.pigeon.util.CommonTools;
import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: mac
 * Date: 11-11-3
 * Time: 下午6:52
 * To change this template use File | Settings | File Templates.
 */
public class PigeonFileSystemConfig {
    Logger logger = Logger.getLogger("PigeonFileSystemConfig");

    public class GlobalGroup {
        public String id;
        public boolean genRelated = true;
        public int priority; //权重，上传文件的时候优先放到权重高的Group
        public LocalGroup preferedLocalGroup;
        public List<LocalGroup> localGroups;
    }

    public class LocalGroup {
        public String id;
        public List<Server> servers;
    }

    public class Server {
        public String serverId;
        public String externalUrl;
        public String[] externalUrls = null;
        public String internalUrl;
        public String writeUrl;
    }

    public List<GlobalGroup> globalGroups;

    public GlobalGroup getGlobalGroup(String serverId) {
        for (GlobalGroup g : globalGroups) {
            for (LocalGroup lg : g.localGroups) {
                for (Server server : lg.servers) {
                    if (server.serverId.equals(serverId)) {
                        return g;
                    }
                }
            }
        }
        return null;
    }

    public List<Server> getServers(String serverId) {
        GlobalGroup g = getGlobalGroup(serverId);
        return getServers(g);
    }

    public String getServerFullID(String serverId) {
        for (GlobalGroup g : globalGroups) {
            for (LocalGroup lg : g.localGroups) {
                for (Server server : lg.servers) {
                    if (server.serverId.equals(serverId)) {
                        return g.id + "_" + lg.id + "_" + server.serverId;
                    }
                }
            }
        }
        return null;
    }

    public Server getServer(String serverId) {
        for (GlobalGroup g : globalGroups) {
            for (LocalGroup lg : g.localGroups) {
                for (Server server : lg.servers) {
                    if (server.serverId.equals(serverId)) {
                        return server;
                    }
                }
            }
        }
        return null;
    }

    static int c = 0;

    Server selectServerFromLocalGroup(LocalGroup lg) {
        int idx = c++ % lg.servers.size(); //随机选一个
        return lg.servers.get(idx);
    }

    Server selectServer(String globalId) {
        for (GlobalGroup g : globalGroups) {
            if (StringUtils.equals(g.id, globalId)) {
                if (g.preferedLocalGroup != null) {
                    return selectServerFromLocalGroup(g.preferedLocalGroup);
                } else {
                    return selectServerFromLocalGroup(g.localGroups.get(c++ % g.localGroups.size()));//随机选一个
                }
            }
        }
        // System.out.println("PigeonFileSystemConfig selectServer() == null, globalId = " + globalId);
        return null;
    }

    Server selectOrigServer(String globalId, String lgID, String fsID) {
        for (GlobalGroup g : globalGroups) {
            if (StringUtils.equals(g.id, globalId)) {
                for (LocalGroup lg : g.localGroups) {
                    if (StringUtils.equals(lg.id, lgID)) {
                        for (Server s : lg.servers) {
                            if (StringUtils.equals(s.serverId, fsID)) {
                                return s;
                            }
                        }
                    }
                }
            }
        }
        Server s = selectServer(globalId);
        if (s != null) {
            return s;
        }
        System.out.println("PigeonFileSystemConfig selectServer() == null, globalId = " + globalId);
        return null;
    }

    public List<Server> getServers(GlobalGroup g) {
        ArrayList result = new ArrayList();
        for (LocalGroup lg : g.localGroups) {
            result.addAll(lg.servers);
        }
        return result;
    }

    public PigeonFileSystemConfig(String config) throws Exception {
        this(config, false);
    }


    void init(JSONObject jconfig) throws Exception {
        JSONArray jsoGroups = jconfig.getJSONArray("globalgroups");
        globalGroups = new Vector<GlobalGroup>();
        for (int i = 0; i < jsoGroups.length(); i++) {
            JSONObject jglobal = jsoGroups.getJSONObject(i);
            String name = jglobal.getString("name");
            GlobalGroup globalGroup = new GlobalGroup();
            if (jglobal.optString("genRelated").equalsIgnoreCase("no")) {
                globalGroup.genRelated = false;
                System.out.println("PigeonFileSystem genRelated = false");
            }
            globalGroup.id = name;
            globalGroup.priority = jglobal.getInt("priority");
            globalGroup.localGroups = new Vector<LocalGroup>();
            globalGroups.add(globalGroup);
            String PreferredLocalGroupId = jglobal.getString("preferredLocalGroup");
            JSONArray jsoLocalGroups = jglobal.getJSONArray("localgroups");
            //读入LocalGroups
            for (int j = 0; j < jsoLocalGroups.length(); j++) {
                JSONObject jlocal = (JSONObject) jsoLocalGroups.get(j);
                LocalGroup localGroup = new LocalGroup();
                localGroup.id = jlocal.getString("id");
                if (localGroup.id.equals(PreferredLocalGroupId)) {
                    globalGroup.preferedLocalGroup = localGroup;
                }
                localGroup.servers = new Vector<Server>();
                globalGroup.localGroups.add(localGroup);
                //读入servers
                JSONArray jsoServers = jlocal.getJSONArray("servers");
                for (int k = 0; k < jsoServers.length(); k++) {
                    JSONObject jserver = (JSONObject) jsoServers.get(k);
                    Server server = new Server();
                    server.internalUrl = jserver.getString("internalUrl");
                    server.externalUrl = jserver.getString("externalUrl");
                    String externalUrls = jserver.optString("externalUrls");
                    if (externalUrls.length() > 0) {
                        server.externalUrls = externalUrls.split(",");
                    }
                    server.writeUrl = jserver.getString("writeUrl");
                    server.serverId = jserver.getString("serverId");
                    localGroup.servers.add(server);
                }
            }
        }
    }

    void init(InputStream in) throws Exception {
        byte[] bytes = CommonTools.getAllBytes(in);
        String s = new String(bytes, "UTF-8");
        JSONObject jso = new JSONObject(s);
        init(jso);

    }

    public PigeonFileSystemConfig(String config, boolean asResource) throws Exception {
        System.out.println("pigeon filesystem client config = " + config);
        if (config.startsWith("@")) {
            asResource = true;
            config = config.substring(1);
        }
        if (asResource) {
            InputStream in = getClass().getResourceAsStream(config);
            init(in);
            in.close();
        } else {
            File f = new File(config);
            f = new File(f.getAbsolutePath());
            if (!f.exists()) {
                logger.log(Level.SEVERE, "config file '" + f.getAbsolutePath() + ",not found");
                throw new Exception("config file not found");
            }
            //如果存在
            FileInputStream fis = new FileInputStream(f);
            init(fis);
            fis.close();
        }
    }

    //GlobalGroup
    //selectGlobalGroup
    //按一定的逻辑选择GlobalGorup
    //目前主要会按照每个GlobalGroup的文件数来选择
    int count = 0;

    PigeonFileSystemConfig.GlobalGroup selectGlobalGroupForUpload() {
        int max = 0;
        List<PigeonFileSystemConfig.GlobalGroup> groups = new Vector<PigeonFileSystemConfig.GlobalGroup>();
        for (PigeonFileSystemConfig.GlobalGroup g : globalGroups) {
            if (g.priority > max) {
                groups.clear();
                groups.add(g);
                max = g.priority;
            } else if (g.priority == max) {
                groups.add(g);
            }
        }
        count++;
        return groups.get(count % groups.size());
    }


}
