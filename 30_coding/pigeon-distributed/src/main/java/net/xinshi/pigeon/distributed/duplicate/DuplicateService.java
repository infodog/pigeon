package net.xinshi.pigeon.distributed.duplicate;

import net.xinshi.pigeon.distributed.Constants;
import net.xinshi.pigeon.distributed.bean.DataItem;
import net.xinshi.pigeon.distributed.bean.PigeonNode;
import net.xinshi.pigeon.distributed.bean.ServerConfig;
import net.xinshi.pigeon.netty.client.Client;
import net.xinshi.pigeon.netty.common.PigeonFuture;
import net.xinshi.pigeon.util.CommonTools;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-20
 * Time: 上午10:11
 * To change this template use File | Settings | File Templates.
 */

public class DuplicateService {

    public static DuplicateConfig duplicateConfig;
    public static Map<Object, ServerConfig> mapNodeServerConfig = new HashMap<Object, ServerConfig>();
    public static Map<ServerConfig, DuplicateServer> mapScDupServer = new HashMap<ServerConfig, DuplicateServer>();
    public static Map<DuplicateServer, SyncDataManager> mapDsSyncManager = new HashMap<DuplicateServer, SyncDataManager>();

    public static DuplicateConfig getDuplicateConfig() {
        return duplicateConfig;
    }

    public static void setDuplicateConfig(DuplicateConfig duplicateConfig) {
        DuplicateService.duplicateConfig = duplicateConfig;
    }

    public static void linkNodeServerConfig(Object node, ServerConfig sc) {
        mapNodeServerConfig.put(node, sc);
    }

    public static void initDuplicateServer(Object obj, ServerConfig sc) {
        try {
            //TODO:zxy整个的复制，需要仔细的检查代码
            DuplicateConfig duplicateConfig = DuplicateService.getDuplicateConfig();
            if(duplicateConfig==null){
                return;
            }
            DuplicateService.linkNodeServerConfig(obj, sc);
            DuplicateServer ds = new DuplicateServer(sc);
            if (duplicateConfig != null) {
                ds.init(duplicateConfig.getNodesManager().getPigeonTypes());
            } else {
                ds.setRole('M');
            }
            ds.printNodes();
            DuplicateService.getMapScDupServer().put(sc, ds);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static DuplicateServer getDuplicateServerByServerConfig(ServerConfig sc) {
        return DuplicateService.getMapScDupServer().get(sc);
    }

    public static Map<Object, ServerConfig> getMapNodeServerConfig() {
        return mapNodeServerConfig;
    }

    public static Map<ServerConfig, DuplicateServer> getMapScDupServer() {
        return mapScDupServer;
    }

    public static boolean isMaster(Object node) {
        ServerConfig sc = mapNodeServerConfig.get(node);
        if (sc != null) {
            DuplicateServer ds = mapScDupServer.get(sc);
            if (ds != null) {
                return ds.getRole() == 'M';
            }
        }
        return true;
    }

    public static boolean syncQueueOverflow(Object node) throws Exception {
        ServerConfig sc = mapNodeServerConfig.get(node);
        if (sc != null) {
            DuplicateServer ds = mapScDupServer.get(sc);
            if (ds != null) {
                SyncDataManager sdm = null;
                synchronized (ds) {
                    sdm = mapDsSyncManager.get(ds);
                    if (sdm == null) {
                        sdm = new SyncDataManager(node);
                        sdm.init();
                        mapDsSyncManager.put(ds, sdm);
                    }
                }
                return sdm.syncQueueOverflow();
            }
        }
        return false;
    }

    public static DataItem duplicateData(Object node, long version, byte[] data) throws Exception {
        ServerConfig sc = mapNodeServerConfig.get(node);
        if (sc == null) {
            return null;
        }
        DuplicateServer ds = mapScDupServer.get(sc);
        if (ds == null) {
            return null;
        }
        SyncDataManager sdm = null;
        synchronized (ds) {
            sdm = mapDsSyncManager.get(ds);
            if (sdm == null) {
                sdm = new SyncDataManager(node);
                sdm.init();
                mapDsSyncManager.put(ds, sdm);
            }
        }
        DataItem di = new DataItem(version, data);
        if (sdm.pushDataItem(di)) {
            return di;
        }
        throw new Exception("duplicateData pushDataItem() == false");
    }

    public static InputStream pullData(Object obj, long minVersion, long maxVersion) throws Exception {
        ServerConfig sc = mapNodeServerConfig.get(obj);
        if (sc == null) {
            return null;
        }
        DuplicateServer ds = mapScDupServer.get(sc);
        if (ds == null || ds.getListMasterPNs().size() < 1) {
            return null;
        }
        PigeonNode node = ds.getListMasterPNs().get(0);
        Client c = DuplicateService.getClient(node);
        if (c == null) {
            throw new Exception(sc.getNodeName() + " pullData() PigeonNode Client == null ");
        }
        int t = 0x0;
        int no = Integer.valueOf(node.getName().substring(node.getType().length())) & 0xFF;
        if (node.getType().equals("flexobject")) {
            t = 0x1;
        } else if (node.getType().equals("list")) {
            t = 0x2;
        } else if (node.getType().equals("atom")) {
            t = 0x3;
        } else if (node.getType().equals("idserver")) {
            t = 0x4;
        } else if (node.getType().equals("lock")) {
            t = 0x5;
        }
        if (t == 0x0) {
            throw new Exception(sc.getNodeName() + " pullData() PigeonNode type error ... ");
        }
        int flag = (((int) t << 8) | (int) no) & 0xFFFF | 0x2000;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeLong(out, minVersion);
        CommonTools.writeLong(out, maxVersion);
        byte[] data = out.toByteArray();
        PigeonFuture pf = c.send((short) flag, data);
        if (pf == null) {
            pf = c.send((short) flag, data);
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
            throw new Exception(sc.getNodeName() + " pullData() netty commit pf == null");
        }
        if (!ok) {
            throw new Exception(sc.getNodeName() + " pullData() netty commit server timeout");
        }
        InputStream is = new ByteArrayInputStream(pf.getData(), Constants.PACKET_PREFIX_LENGTH, pf.getData().length - Constants.PACKET_PREFIX_LENGTH);
        return is;
    }

    public static long masterVersion(Object obj) throws Exception {
        ServerConfig sc = mapNodeServerConfig.get(obj);
        if (sc == null) {
            return -1L;
        }
        DuplicateServer ds = mapScDupServer.get(sc);
        if (ds == null || ds.getListMasterPNs().size() < 1) {
            return -1L;
        }
        PigeonNode node = ds.getListMasterPNs().get(0);
        Client c = DuplicateService.getClient(node);
        if (c == null) {
            throw new Exception(sc.getNodeName() + " masterVersion() PigeonNode Client == null ");
        }
        int t = 0x0;
        int no = Integer.valueOf(node.getName().substring(node.getType().length())) & 0xFF;
        if (node.getType().equals("flexobject")) {
            t = 0x1;
        } else if (node.getType().equals("list")) {
            t = 0x2;
        } else if (node.getType().equals("atom")) {
            t = 0x3;
        } else if (node.getType().equals("idserver")) {
            t = 0x4;
        } else if (node.getType().equals("lock")) {
            t = 0x5;
        }
        if (t == 0x0) {
            throw new Exception(sc.getNodeName() + " masterVersion() PigeonNode type error ... ");
        }
        int flag = (((int) t << 8) | (int) no) & 0xFFFF | 0xF000;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "version");
        byte[] data = out.toByteArray();
        PigeonFuture pf = c.send((short) flag, data);
        if (pf == null) {
            pf = c.send((short) flag, data);
        }
        boolean ok = false;
        try {
            if (pf != null) {
                ok = pf.waitme(1000 * 30);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (pf == null) {
            throw new Exception(sc.getNodeName() + " masterVersion() netty commit pf == null");
        }
        if (!ok) {
            throw new Exception(sc.getNodeName() + " masterVersion() netty commit server timeout");
        }
        InputStream is = new ByteArrayInputStream(pf.getData(), Constants.PACKET_PREFIX_LENGTH, pf.getData().length - Constants.PACKET_PREFIX_LENGTH);
        String ver = CommonTools.readString(is);
        return Long.valueOf(ver);
    }

    public static int appendDataItems(Object node, long min, long max, OutputStream out) throws Exception {
        ServerConfig sc = mapNodeServerConfig.get(node);
        if (sc == null) {
            return 0;
        }
        DuplicateServer ds = mapScDupServer.get(sc);
        if (ds == null) {
            return 0;
        }
        SyncDataManager sdm = null;
        synchronized (ds) {
            sdm = mapDsSyncManager.get(ds);
        }
        if (sdm == null) {
            return 0;
        }
        return sdm.appendDataItems(min, max, out);
    }

    public static void updateListDataItems(Object node, long version) throws Exception {
        ServerConfig sc = mapNodeServerConfig.get(node);
        if (sc == null) {
            return;
        }
        DuplicateServer ds = mapScDupServer.get(sc);
        if (ds == null) {
            return;
        }
        SyncDataManager sdm = null;
        synchronized (ds) {
            sdm = mapDsSyncManager.get(ds);
        }
        if (sdm == null) {
            return;
        }
        sdm.updateListDataItems(version);
    }

    public static Client getClient(PigeonNode node) {
        Client c = duplicateConfig.PigeonNodes.get(node.getHost() + ":" + node.getPort());
        return c;
    }

}

