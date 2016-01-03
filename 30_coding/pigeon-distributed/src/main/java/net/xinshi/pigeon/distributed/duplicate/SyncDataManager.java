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
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-21
 * Time: 下午12:10
 * To change this template use File | Settings | File Templates.
 */

public class SyncDataManager {

    final int MAX_SYNC_QUEUE = 1000;
    final int MAX_FLUSH_DB_QUEUE = 100000;
    final int ALARM_DB_QUEUE = (int) (MAX_FLUSH_DB_QUEUE * 0.999f);
    Object ownerPigeonNode;
    ServerConfig sc;
    DuplicateServer ds;
    LinkedBlockingQueue<DataItem> queueSyncDataItems;
    LinkedBlockingQueue<DataItem> queueBackupDataItems;
    List<DataItem> listDataItems;
    boolean bSync = false;
    boolean bBackup = false;

    public SyncDataManager(Object ownerPigeonNode) {
        this.ownerPigeonNode = ownerPigeonNode;
        queueSyncDataItems = new LinkedBlockingQueue<DataItem>();
        queueBackupDataItems = new LinkedBlockingQueue<DataItem>();
        listDataItems = new LinkedList<DataItem>();
    }

    public int appendDataItems(long min, long max, OutputStream out) throws Exception {
        int n = 0;
        synchronized (listDataItems) {
            for (DataItem di : listDataItems) {
                if (di.getVersion() >= min && di.getVersion() <= max) {
                    CommonTools.writeLong(out, di.getVersion());
                    CommonTools.writeBytes(out, di.getData(), 0, di.getData().length);
                    ++n;
                } else if (di.getVersion() > max) {
                    break;
                }
            }
        }
        if (n > 0) {
            System.out.println("SyncDataManager appendDataItems() n = " + n);
        }
        return n;
    }

    public void updateListDataItems(long version) {
        synchronized (listDataItems) {
            while (listDataItems.size() > 0 && listDataItems.get(0).getVersion() <= version) {
                listDataItems.remove(0);
            }
        }
    }

    public boolean syncQueueOverflow() {
        synchronized (queueSyncDataItems) {
            if (queueSyncDataItems.size() > MAX_SYNC_QUEUE) {
                return true;
            }
        }
        synchronized (listDataItems) {
            if (listDataItems.size() > MAX_FLUSH_DB_QUEUE) {
                return true;
            }
        }
        return false;
    }

    public boolean pushDataItem(DataItem di) {
        synchronized (listDataItems) {
            listDataItems.add(di);
            if (listDataItems.size() > ALARM_DB_QUEUE) {
                System.out.println(sc.getNodeName() + " listDataItems size() == " + listDataItems.size());
            }
        }
        if (!bSync) {
            synchronized (di) {
                di.setComplete(true);
            }
            return true;
        }
        synchronized (queueSyncDataItems) {
            if (queueSyncDataItems.size() > MAX_SYNC_QUEUE) {
                System.out.println(sc.getNodeName() + " pushDataItem overflow ...... ");
                return false;
            }
            return queueSyncDataItems.add(di);
        }
    }

    public void init() throws Exception {
        sc = DuplicateService.mapNodeServerConfig.get(ownerPigeonNode);
        if (sc == null) {
            throw new Exception("SyncDataManager init ServerConfig == null");
        }
        ds = DuplicateService.getMapScDupServer().get(sc);
        if (ds == null) {
            throw new Exception("SyncDataManager init DuplicateHandler == null");
        }
        new Thread(new syncWorker()).start();
        new Thread(new backupWorker()).start();
    }

    class syncWorker implements Runnable {

        boolean syncData(byte[] data) throws Exception {
            PigeonNode node = ds.getListSyncPNs().get(0);
            Client c = DuplicateService.getClient(node);
            if (c == null) {
                throw new Exception(sc.getNodeName() + " syncData() PigeonNode Client == null ");
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
                throw new Exception(sc.getNodeName() + " syncData() PigeonNode type error ... ");
            }
            int flag = (((int) t << 8) | (int) no) & 0xFFFF | 0x8000;
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
                throw new Exception(sc.getNodeName() + " syncData() netty commit pf == null");
            }
            if (!ok) {
                throw new Exception(sc.getNodeName() + " syncData() netty commit server timeout");
            }
            InputStream in = new ByteArrayInputStream(pf.getData(), Constants.PACKET_PREFIX_LENGTH, pf.getData().length - Constants.PACKET_PREFIX_LENGTH);
            String status = CommonTools.readString(in);
            if (status.equals("ok")) {
                return true;
            } else {
                System.out.println(sc.getNodeName() + " " + status);
                return false;
            }
        }

        public void run() {
            if (ds.getListSyncPNs().size() < 1) {
                System.out.println(sc.getNodeName() + " ds.getListSyncPNs() == 0");
                return;
            }
            Thread.currentThread().setName("syncWorker_run");
            bSync = true;
            Vector<DataItem> vecDIs = new Vector<DataItem>(MAX_SYNC_QUEUE * 2);
            while (true) {
                DataItem di = null;
                if (vecDIs.size() == 0) {
                    try {
                        di = queueSyncDataItems.take();
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                    vecDIs.add(di);
                    synchronized (queueSyncDataItems) {
                        vecDIs.addAll(queueSyncDataItems);
                        queueSyncDataItems.clear();
                    }
                }
                try {
                    ByteArrayOutputStream body = new ByteArrayOutputStream(128 * vecDIs.size());
                    for (DataItem d : vecDIs) {
                        CommonTools.writeLong(body, d.getVersion());
                        CommonTools.writeBytes(body, d.getData(), 0, d.getData().length);
                    }
                    if (syncData(body.toByteArray())) {
                        for (DataItem d : vecDIs) {
                            synchronized (d) {
                                d.setComplete(true);
                                d.notify();
                            }
                        }
                        if (bBackup) {
                            synchronized (queueBackupDataItems) {
                                if (queueBackupDataItems.size() < MAX_SYNC_QUEUE * 10) {
                                    queueBackupDataItems.addAll(vecDIs);
                                }
                            }
                        }
                        vecDIs.clear();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    class backupWorker implements Runnable {

        boolean backupData(byte[] data) throws Exception {
            PigeonNode node = ds.getListBackupPNs().get(0);
            Client c = DuplicateService.getClient(node);
            if (c == null) {
                throw new Exception(sc.getNodeName() + " backupData() PigeonNode Client == null ");
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
                throw new Exception(sc.getNodeName() + " backupData() PigeonNode type error ... ");
            }
            int flag = (((int) t << 8) | (int) no) & 0xFFFF | 0x4000;
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
                throw new Exception(sc.getNodeName() + " backupData() netty commit pf == null");
            }
            if (!ok) {
                throw new Exception(sc.getNodeName() + " backupData() netty commit server timeout");
            }
            InputStream in = new ByteArrayInputStream(pf.getData(), Constants.PACKET_PREFIX_LENGTH, pf.getData().length - Constants.PACKET_PREFIX_LENGTH);
            String status = CommonTools.readString(in);
            if (status.equals("ok")) {
                return true;
            } else {
                System.out.println(sc.getNodeName() + " " + status);
                return false;
            }
        }

        public void run() {
            if (ds.getListBackupPNs().size() < 1) {
                System.out.println(sc.getNodeName() + " ds.getListBackupPNs() == 0");
                return;
            }
            Thread.currentThread().setName("backupWorker_run");
            bBackup = true;
            Vector<DataItem> vecDIs = new Vector<DataItem>(MAX_SYNC_QUEUE * 20);
            while (true) {
                DataItem di = null;
                if (vecDIs.size() == 0) {
                    try {
                        di = queueBackupDataItems.take();
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                    vecDIs.add(di);
                    synchronized (queueBackupDataItems) {
                        vecDIs.addAll(queueBackupDataItems);
                        queueBackupDataItems.clear();
                    }
                }
                boolean ok = false;
                try {
                    ByteArrayOutputStream body = new ByteArrayOutputStream(128 * vecDIs.size());
                    for (DataItem d : vecDIs) {
                        CommonTools.writeLong(body, d.getVersion());
                        CommonTools.writeBytes(body, d.getData(), 0, d.getData().length);
                    }
                    if (backupData(body.toByteArray())) {
                        vecDIs.clear();
                        ok = true;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (!ok) {
                    try {
                        Thread.sleep(1000 * 5);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

}

