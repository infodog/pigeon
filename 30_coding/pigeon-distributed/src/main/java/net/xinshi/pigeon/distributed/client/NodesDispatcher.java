package net.xinshi.pigeon.distributed.client;

import net.xinshi.pigeon.distributed.Constants;
import net.xinshi.pigeon.distributed.bean.HashRange;
import net.xinshi.pigeon.distributed.bean.PigeonNode;
import net.xinshi.pigeon.distributed.util.DefaultHashGenerator;
import net.xinshi.pigeon.distributed.util.JSONConvert;
import net.xinshi.pigeon.netty.client.Client;
import net.xinshi.pigeon.netty.common.PigeonFuture;
import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-5
 * Time: 上午11:04
 * To change this template use File | Settings | File Templates.
 */

public class NodesDispatcher {

    String host;
    int port;
    long version = 0L;
    Client linkControlServer;
    String config = null;
    HashMap<String, List<HashRange>> PigeonTypes = null;
    HashMap<String, Client> PigeonNodes = new HashMap<String, Client>();
    List<ClientNode> kv_M_nodes = new ArrayList<ClientNode>();

    Logger logger = Logger.getLogger(NodesDispatcher.class.getName());
    boolean asResource = false;
    public volatile boolean licOK = true;

    public NodesDispatcher(String config) {
        this.config = config;
    }

    public NodesDispatcher(String config, boolean asResource) {
        this.asResource = asResource;
        this.config = config;
    }

    public NodesDispatcher(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public HashMap<String, List<HashRange>> getPigeonTypes() {
        return PigeonTypes;
    }

    public void setPigeonTypes(HashMap<String, List<HashRange>> pigeonTypes) {
        PigeonTypes = pigeonTypes;
    }

    public HashMap<String, Client> getPigeonNodes() {
        return PigeonNodes;
    }

    public void setPigeonNodes(HashMap<String, Client> pigeonNodes) {
        PigeonNodes = pigeonNodes;
    }

    public synchronized boolean linkPigeonNodes() {
        Set<String> setKEY = new HashSet<String>();
        for (List<HashRange> listHR : PigeonTypes.values()) {
            for (HashRange hr : listHR) {
                for (PigeonNode pn : hr.getMembers()) {
                    if (pn.getRole() != 'M') {
                        continue;
                    }
                    String key = pn.getHost() + ":" + pn.getPort();
                    if (pn.getPort() == 0) {
                        continue;
                    }
                    Client c = PigeonNodes.get(key);
                    if (c == null) {
                        c = new Client(pn.getHost(), pn.getPort(), 30);
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

    public boolean init() {
        try {
            String info = "";
            if (config == null) {
                if (linkControlServer == null) {
                    linkControlServer = new Client(host, port, 1);
                }
                if (!linkControlServer.init()) {
                    return false;
                }
                byte[] tmp = "hello".getBytes();
                PigeonFuture pf = linkControlServer.send((short) Constants.ACTION_NODES_INFO, tmp);
                if (pf == null || !pf.waitme(5000)) {
                    System.out.println("linkControlServer send error");
                    return false;
                }
                info = new String(pf.getData(), Constants.PACKET_PREFIX_LENGTH, pf.getData().length - Constants.PACKET_PREFIX_LENGTH, "UTF-8");
                info = StringUtils.trim(info);
            } else {
                logger.info("pigeon client config = " + config);
                if (config.startsWith("@")) {
                    asResource = true;
                    config = config.substring(1);
                }
                if (asResource) {
                    InputStream in = getClass().getResourceAsStream(config);
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    byte[] buf = new byte[2048];
                    int n = in.read(buf);
                    while (n > 0) {
                        bos.write(buf, 0, n);
                        n = in.read(buf);
                    }
                    in.close();
                    info = new String(bos.toByteArray(), "UTF-8");
                } else {
                    File f = new File(config);
                    logger.info("load pigeon info from file:" + f.getAbsoluteFile());
                    FileInputStream is = new FileInputStream(f);
                    byte[] b = new byte[(int) f.length()];
                    is.read(b);
                    is.close();
                    String s = new String(b, "UTF-8");
                    info = StringUtils.trim(s);
                }
            }
            version = JSONConvert.readPigeonTypesVersion(info);
            System.out.println("pigeon nodes version : " + version);
            PigeonTypes = JSONConvert.String2PigeonTypes(info);
            if (PigeonTypes == null) {
                return false;
            }
            boolean rc = linkPigeonNodes();
            kv_M_nodes = getClientNodes("flexobject");
            Collections.sort(kv_M_nodes);
            return rc;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public String typeString(short t) {
        switch (t) {
            case 0x1:
                return "flexobject";
            case 0x2:
                return "list";
            case 0x3:
                return "atom";
            case 0x4:
                return "idserver";
            case 0x5:
                return "lock";
            default:
                return "bad type";
        }
    }

    public ClientNode dispatchToNode(String type, String key) throws Exception {
        if (!licOK) {
            throw new Exception("pigeon.lic invalid ...... ");
        }
        int hash = DefaultHashGenerator.hash(key);
        PigeonNode pin = null;
        List<HashRange> listHR = PigeonTypes.get(type);
        for (HashRange hr : listHR) {
            if (hash >= hr.getLeftBoundary() && hash <= hr.getRightBoundary()) {
                for (PigeonNode pn : hr.getMembers()) {
                    if (pn.getRole() == 'M') {
                        pin = pn;
                        break;
                    }
                }
                break;
            }
        }
        if (pin != null) {
            if (pin.getName().startsWith(type)) {
                Client c = PigeonNodes.get(pin.getHost() + ":" + pin.getPort());
                if (c == null) {
                    return null;
                }
                int t = 0x0;
                int no = Integer.valueOf(pin.getName().substring(type.length())) & 0xFF;
                if (type.equals("flexobject")) {
                    t = 0x1;
                } else if (type.equals("list")) {
                    t = 0x2;
                } else if (type.equals("atom")) {
                    t = 0x3;
                } else if (type.equals("idserver")) {
                    t = 0x4;
                } else if (type.equals("lock")) {
                    t = 0x5;
                }
                if (t == 0x0) {
                    return null;
                }
                return new ClientNode((short) t, (short) no, c);
            }
        }
        return null;
    }

    public List<ClientNode> getClientNodes(String type) throws Exception {
        if (!licOK) {
            throw new Exception("pigeon.lic invalid ...... ");
        }
        List<PigeonNode> listPigeonNodes = new ArrayList<PigeonNode>();
        for (HashRange hr : PigeonTypes.get(type)) {
            for (PigeonNode pn : hr.getMembers()) {
                if (pn.getRole() == 'M') {
                    listPigeonNodes.add(pn);
                    break;
                }
            }
        }
        List<ClientNode> listClientNodes = new ArrayList<ClientNode>();
        for (PigeonNode pin : listPigeonNodes) {
            if (pin != null) {
                if (pin.getName().startsWith(type)) {
                    Client c = PigeonNodes.get(pin.getHost() + ":" + pin.getPort());
                    if (c == null) {
                        throw new Exception("getClientNodes() : PigeonNode.Client == null");
                    }
                    int t = 0x0;
                    int no = Integer.valueOf(pin.getName().substring(type.length())) & 0xFF;
                    if (type.equals("flexobject")) {
                        t = 0x1;
                    } else if (type.equals("list")) {
                        t = 0x2;
                    } else if (type.equals("atom")) {
                        t = 0x3;
                    } else if (type.equals("idserver")) {
                        t = 0x4;
                    } else if (type.equals("lock")) {
                        t = 0x5;
                    }
                    if (t == 0x0) {
                        throw new Exception("getClientNodes() : bad type");
                    }
                    ClientNode cn = new ClientNode((short) t, (short) no, c);
                    for (ClientNode e : listClientNodes) {
                        if (e.type == cn.type && e.no == cn.no && e.connection == cn.connection) {
                            cn = null;
                            break;
                        }
                    }
                    if (cn != null) {
                        listClientNodes.add(cn);
                    }
                }
            }
        }
        return listClientNodes;
    }

    public InputStream Commit(String type, String key, ByteArrayOutputStream out) {
        InputStream in = null;
        try {
            ClientNode cn = dispatchToNode(type, key);
            if (cn == null) {
                System.out.println("dispatchToNode ClientNode == null");
                return null;
            }
            int flag = (((int) cn.getType() << 8) | (int) cn.getNo()) & 0xFFFF;
            PigeonFuture pf = cn.getConnection().send((short) flag, out.toByteArray());
            if (pf == null) {
                pf = cn.getConnection().send((short) flag, out.toByteArray());
            }
            boolean ok = false;
            try {
                if (pf != null) {
                    ok = pf.waitme(1000 * 60);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (pf == null) {
                String detail = "[" + cn.getConnection().getHost() + ":" + cn.getConnection().getPort() + "/" + typeString(cn.getType()) + cn.getNo() + "]";
                throw new Exception("netty commit pf == null " + detail);
            }
            if (!ok) {
                String detail = "[" + cn.getConnection().getHost() + ":" + cn.getConnection().getPort() + "/" + typeString(cn.getType()) + cn.getNo() + "]";
                throw new Exception("netty commit server timeout " + detail);
            }
            in = new ByteArrayInputStream(pf.getData(), Constants.PACKET_PREFIX_LENGTH, pf.getData().length - Constants.PACKET_PREFIX_LENGTH);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return in;
    }

    public PigeonFuture CommitAsync(String type, String key, ByteArrayOutputStream out) {
        try {
            ClientNode cn = dispatchToNode(type, key);
            if (cn == null) {
                System.out.println("dispatchToNode ClientNode == null");
                return null;
            }
            int flag = (((int) cn.getType() << 8) | (int) cn.getNo()) & 0xFFFF;
            PigeonFuture pf = cn.getConnection().send((short) flag, out.toByteArray());
            if (pf == null) {
                pf = cn.getConnection().send((short) flag, out.toByteArray());
            }
            return pf;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<PigeonFuture> CommitAsync(String type, ByteArrayOutputStream out) throws Exception {
        try {
            List<PigeonFuture> listPigeonFutures = new ArrayList<PigeonFuture>();
            List<ClientNode> listClientNodes = getClientNodes(type);
            for (ClientNode cn : listClientNodes) {
                if (cn == null) {
                    System.out.println("dispatchToNode ClientNode == null");
                    return null;
                }
                int flag = (((int) cn.getType() << 8) | (int) cn.getNo()) & 0xFFFF;
                PigeonFuture pf = cn.getConnection().send((short) flag, out.toByteArray());
                if (pf == null) {
                    pf = cn.getConnection().send((short) flag, out.toByteArray());
                }
                listPigeonFutures.add(pf);
            }
            return listPigeonFutures;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<ClientNode> get_kv_m_nodes() {
        return kv_M_nodes;
    }

}

