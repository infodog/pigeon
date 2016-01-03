package net.xinshi.pigeon.dumpload.migration;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.distributed.util.DefaultHashGenerator;
import net.xinshi.pigeon.list.ISortList;
import net.xinshi.pigeon.list.SortListObject;
import net.xinshi.pigeon.util.TimeTools;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-4-11
 * Time: 下午3:18
 * To change this template use File | Settings | File Templates.
 */

public class MigrationList {
    static long count = -1;
    DataSource ds;
    IPigeonStoreEngine pigeonStoreEngine;
    String listTable;

    public MigrationList(DataSource ds, IPigeonStoreEngine pigeonStoreEngine, String listTable) {
        this.ds = ds;
        this.pigeonStoreEngine = pigeonStoreEngine;
        this.listTable = listTable;
    }

    long getRecordsNumber() throws Exception {
        System.out.println("sortlist do getRecordsNumber() ... ");
        long count = -1;
        String sql = "select count(*) as count from " + listTable + " where ismeta=0";
        Connection conn = null;
        try {
            conn = ds.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                count = rs.getLong("count");
            }
            rs.close();
            stmt.close();
            return count;
        } finally {
            if (conn != null && conn.isClosed() == false) {
                conn.close();
            }
        }
    }

    class key_val {
        public String key;
        public String val;

        key_val(String key, String val) {
            this.key = key;
            this.val = val;
        }
    }

    LinkedBlockingQueue<key_val> queKVs = new LinkedBlockingQueue<key_val>();

    List fetchRecords(int begin, int count) throws Exception {
        List<key_val> records = new ArrayList<key_val>();
        // String sql = "select listName,value  from " + listTable + " where ismeta=0 order by listName limit " + begin + "," + count;
        // String sql = "select listName,value  from " + listTable + " where ismeta=0 limit " + begin + "," + count;
        String sql = "select * from (select listName,value from " + listTable + " where isMeta=0 order by listName) t limit " + begin + "," + count;
        Connection conn = null;
        try {
            conn = ds.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                byte[] bytes = rs.getBytes("listName");
                String listname = null;
                if (bytes != null) {
                    listname = new String(bytes, "UTF-8");
                }
                bytes = rs.getBytes("value");
                String value = null;
                if (bytes != null) {
                    value = new String(bytes, "UTF-8");
                }
                records.add(new key_val(listname, value));
            }
            rs.close();
            stmt.close();
            if (records.size() != count) {
                if (begin + records.size() < MigrationList.count) {
                    System.out.println("list records.size() != count size= " + records.size() + " , sql : " + sql);
                }
            }
            return records;
        } finally {
            if (conn != null && conn.isClosed() == false) {
                conn.close();
            }
        }
    }

    private class QueryDB extends Thread {

        public void run() {
            int pos = 0;
            System.out.println("begin query db ...");
            while (pos < count) {
                if (queKVs.size() < 5000) {
                    int s = (int) count - pos;
                    if (s > 10000) {
                        s = 10000;
                    }
                    try {
                        List<key_val> records = fetchRecords((int) pos, (int) s);
                        if (records.size() != s) {
                            System.out.println("panic ... fetchRecords get = " + records.size() + ", want = " + s);
                            break;
                        }
                        pos += s;
                        synchronized (queKVs) {
                            queKVs.addAll(records);
                        }
                        System.out.println(TimeTools.getNowTimeString() + " ****** QueryDB, end pos = " + pos + ", size = " + s + ", Queue = " + queKVs.size() + ", left = " + (count - pos));
                    } catch (Exception e) {
                        System.out.println("panic ... pos = " + pos + ", s = " + s);
                        e.printStackTrace();
                        // break;
                    }
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            for (int i = 0; i < 1000; i++) {
                synchronized (queKVs) {
                    queKVs.add(new key_val(null, null));
                }
            }
            System.out.println("query db finished .....");
        }
    }

    public void init() throws Exception {
        long st = System.currentTimeMillis();
        count = getRecordsNumber();
        if (count < 0) {
            throw new Exception("list getRecordsNumber() error ");
        }
        if (count < 1) {
            System.out.println("sortlist records count == 0");
            return;
        }
        System.out.println("...... sortlist records count == " + count);
        QueryDB qdb = new QueryDB();
        qdb.start();
        int nw = 50;
        long one = (count + (nw - 1)) / nw;
        CountDownLatch runningThreadNum = null;
        if (one < 1) {
            runningThreadNum = new CountDownLatch(1);
            Migration m = new Migration(runningThreadNum);
            m.start();
        } else {
            runningThreadNum = new CountDownLatch(nw);
            for (int i = 0; i < nw; i++) {
                Migration m = new Migration(runningThreadNum);
                m.start();
            }
        }
        runningThreadNum.await();
        long et = System.currentTimeMillis();
        System.out.println("...... sortlist migration finished time ms = " + (et - st));
    }

    List<key_val> getKVs() throws Exception {
        List<key_val> listKV = new ArrayList<key_val>();
        key_val kv = queKVs.take();
        listKV.add(kv);
        return listKV;
    }

    private class Migration extends Thread {

        private CountDownLatch runningThreadNum;

        public Migration(CountDownLatch runningThreadNum) {
            this.runningThreadNum = runningThreadNum;
        }

        public void run() {
            String threadid = Thread.currentThread().getName() + " ------ ";
            long st = System.currentTimeMillis();
            System.out.println(threadid + "sortlist thread begin");
            int nn = 0;
            int num = 0;
            long t1 = System.currentTimeMillis();
            boolean again = true;
            while (again) {
                String key = "";
                try {
                    List<key_val> records = getKVs();
                    for (key_val elt : records) {
                        if (elt == null || elt.key == null) {
                            again = false;
                            break;
                        }
                        key = elt.key;
                        {
                            int hash = DefaultHashGenerator.hash(key);
                            if (!PigeonMigration.rightRange(hash)) {
                                System.out.println(threadid + " ?????? sortlist key bad hash range , key = " + key + ", hash = " + hash);
                                continue;
                            }
                        }
                        String[] strObjs = elt.val.split(";");
                        for (String strObj : strObjs) {
                            String[] fields = strObj.split(",");
                            if (fields.length == 2) {
                                SortListObject obj = new SortListObject();
                                obj.setObjid(fields[0]);
                                obj.setKey(fields[1]);
                                while (true) {
                                    try {
                                        ISortList list = pigeonStoreEngine.getListFactory().getList(key, true);
                                        boolean rb = list.add(obj);
                                        if (!rb) {
                                            // throw new Exception("list Add SortObj failed ... " + key);
                                        }
                                        break;
                                    } catch (Exception e) {
                                        if (e.getMessage().indexOf("DuplicateService.syncQueueOverflow()") >= 0) {
                                            // System.out.println("migration list : server is very busy ......");
                                            Thread.sleep(1000);
                                        } else {
                                            // throw e;
                                            e.printStackTrace();
                                            System.out.println(key + " <-- key : " + e.getMessage());
                                            Thread.sleep(1000 * 5);
                                        }
                                    }
                                }
                                ++nn;
                                ++num;
                            }
                        }
                    }
                    if (nn > 1000) {
                        // System.out.println(threadid + " loop list finished " + nn);
                        long t2 = System.currentTimeMillis();
                        System.out.println(threadid + " add " + nn + " listobj , cost ms = " + (t2 - t1));
                        nn = 0;
                        t1 = t2;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    if (key.length() > 0) {
                        System.out.println(threadid + "panic  !!!!!! sortlist panic ...... key : " + key + " error ");
                    }
                    break;
                }
            }
            long et = System.currentTimeMillis();
            System.out.println(threadid + "sortlist one thread finished time ms = " + (et - st) + " , count = " + num);
            runningThreadNum.countDown();
        }
    }
}

