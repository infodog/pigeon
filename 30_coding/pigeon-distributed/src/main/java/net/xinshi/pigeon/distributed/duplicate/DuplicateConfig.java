package net.xinshi.pigeon.distributed.duplicate;

import net.xinshi.pigeon.distributed.bean.HashRange;
import net.xinshi.pigeon.distributed.bean.PigeonNode;
import net.xinshi.pigeon.distributed.manager.PigeonNodesManager;
import net.xinshi.pigeon.netty.client.Client;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-17
 * Time: 上午10:42
 * To change this template use File | Settings | File Templates.
 */

public class DuplicateConfig {

    String config = null;
    PigeonNodesManager nodesManager;
    HashMap<String, Client> PigeonNodes;
    static Logger logger = Logger.getLogger(DuplicateConfig.class.getName());

    public DuplicateConfig(String config) {
        this.config = config;
        nodesManager = new PigeonNodesManager();
        PigeonNodes = new HashMap<String, Client>();
    }

    public PigeonNodesManager getNodesManager() {
        return nodesManager;
    }

    public boolean linkPigeonNodes() {
        Set<String> setKEY = new HashSet<String>();
        for (List<HashRange> listHR : nodesManager.getPigeonTypes().values()) {
            for (HashRange hr : listHR) {
                for (PigeonNode pn : hr.getMembers()) {
                    String key = pn.getHost() + ":" + pn.getPort();
                    if (pn.getPort() == 0) {
                        continue;
                    }
                    Client c = PigeonNodes.get(key);
                    if (c == null) {
                        c = new Client(pn.getHost(), pn.getPort(), 20);
                        c.init();
                        PigeonNodes.put(key, c);
                    }
                    setKEY.add(key);
                }
            }
        }
        for (String key : PigeonNodes.keySet()) {
            if (!setKEY.contains(key)) {
                // Client clean;
                // PigeonNodes.remove(key);
            }
        }
        return setKEY.size() > 0;
    }

    public synchronized boolean init() {
        try {
            String info = "";
            if (config != null) {
                nodesManager.init(config);
                boolean rb = linkPigeonNodes();
                System.out.println("DuplicateController do INIT ... ");
                return rb;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

}

