package net.xinshi.pigeon.saas.datasource;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.adapter.impl.DistributedPigeonEngine;
import net.xinshi.pigeon.distributed.Constants;
import net.xinshi.pigeon.distributed.client.ClientNode;
import net.xinshi.pigeon.netty.client.Client;
import net.xinshi.pigeon.netty.common.PigeonFuture;
import net.xinshi.pigeon.persistence.VersionHistory;
import net.xinshi.pigeon.persistence.VersionHistoryLogger;
import net.xinshi.pigeon.util.CommonTools;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: WPF
 * Date: 13-1-28
 * Time: 上午10:04
 * To change this template use File | Settings | File Templates.
 */

public class PigeonDataSource {

    public static IPigeonStoreEngine pigeonStoreEngine;

    public static void setPigeon(IPigeonStoreEngine pigeonStoreEngine) {
        PigeonDataSource.pigeonStoreEngine = pigeonStoreEngine;
    }

    public static int getDataSourceCount() throws Exception {
        if (!(pigeonStoreEngine instanceof DistributedPigeonEngine)) {
            throw new Exception("PigeonDataSource ... (pigeon not instanceof DistributedPigeonEngine)");
        }
        DistributedPigeonEngine dpe = (DistributedPigeonEngine) pigeonStoreEngine;
        return dpe.get_kv_m_nodes().size();
    }

    private static long masterVersion(ClientNode cn) throws Exception {
        Client c = cn.getConnection();
        if (c == null) {
            throw new Exception("ClientNode Client == null");
        }
        int t = cn.getType();
        int no = cn.getNo() & 0xFF;
        if (t == 0x0) {
            throw new Exception("ClientNode type error ... ");
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
            String detail = "[" + c.getHost() + ":" + c.getPort() + "/" + cn.getType() + cn.getNo() + "]";
            throw new Exception("ClientNode netty commit pf == null " + detail);
        }
        if (!ok) {
            String detail = "[" + c.getHost() + ":" + c.getPort() + "/" + cn.getType() + cn.getNo() + "]";
            throw new Exception("ClientNode netty commit server timeout " + detail);
        }
        InputStream is = new ByteArrayInputStream(pf.getData(), Constants.PACKET_PREFIX_LENGTH, pf.getData().length - Constants.PACKET_PREFIX_LENGTH);
        String ver = CommonTools.readString(is);
        return Long.valueOf(ver);
    }

    public static long getLastVersion(int DataSourceID) throws Exception {
        if (getDataSourceCount() <= DataSourceID) {
            return -1L;
        }
        DistributedPigeonEngine dpe = (DistributedPigeonEngine) pigeonStoreEngine;
        return masterVersion(dpe.get_kv_m_nodes().get(DataSourceID));
    }

    private static List<VersionHistory> getVersionHistory(ClientNode cn, long minVersion, long maxVersion) throws Exception {
        Client c = cn.getConnection();
        if (c == null) {
            throw new Exception("ClientNode Client == null");
        }
        int t = cn.getType();
        int no = cn.getNo() & 0xFF;
        if (t == 0x0) {
            throw new Exception("ClientNode type error ... ");
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
            throw new Exception("getVersionHistory pullData() netty commit pf == null");
        }
        if (!ok) {
            throw new Exception("getVersionHistory pullData() netty commit server timeout");
        }
        InputStream is = new ByteArrayInputStream(pf.getData(), Constants.PACKET_PREFIX_LENGTH, pf.getData().length - Constants.PACKET_PREFIX_LENGTH);
        List<VersionHistory> listVHs = new LinkedList<VersionHistory>();
        while (true) {
            long v = 0L;
            byte[] buf = null;
            try {
                v = CommonTools.readLong(is);
                buf = CommonTools.readBytes(is);
                listVHs.add(new VersionHistory(buf.length, buf, buf.length, v, VersionHistoryLogger.TailMagicNumber));
            } catch (Exception e) {
                break;
            }
        }
        return listVHs;
    }

    public static List<VersionHistory> getVersionHistory(int DataSourceID, long begin, long end) throws Exception {
        if (begin < 1) {
            begin = 1;
        }
        if (end < begin || getDataSourceCount() <= DataSourceID) {
            return null;
        }
        DistributedPigeonEngine dpe = (DistributedPigeonEngine) pigeonStoreEngine;
        return getVersionHistory(dpe.get_kv_m_nodes().get(DataSourceID), begin, end);
    }

}
