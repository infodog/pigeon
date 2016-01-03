package net.xinshi.pigeon.dumpload.migration;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.distributed.util.DefaultHashGenerator;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-4-11
 * Time: 下午4:55
 * To change this template use File | Settings | File Templates.
 */

public class MigrationIDGen {
    static long count = -1;
    DataSource ds;
    IPigeonStoreEngine pigeonStoreEngine;
    String tableName;

    public MigrationIDGen(DataSource ds, IPigeonStoreEngine pigeonStoreEngine) {
        this.ds = ds;
        this.pigeonStoreEngine = pigeonStoreEngine;
        this.tableName = "t_ids";
    }

    long getRecordsNumber() throws Exception {
        System.out.println("ids do getRecordsNumber() ... ");
        long count = -1;
        String sql = "select count(*) as count from " + tableName + " ";
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

    List fetchRecords(int begin, int count) throws Exception {
        List<key_val> records = new ArrayList<key_val>();
        // String sql = "select *  from " + tableName + " order by TableName  limit " + begin + "," + count;
        String sql = "select t.* from (select TableName,NextValue from " + tableName + " order by TableName) t limit " + begin + "," + count;
        Connection conn = null;
        try {
            conn = ds.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                byte[] bytes = rs.getBytes("TableName");
                String listname = null;
                if (bytes != null) {
                    listname = new String(bytes, "UTF-8");
                }
                long value = rs.getLong("NextValue");
                records.add(new key_val(listname, String.valueOf(value)));
            }
            rs.close();
            stmt.close();
            if (records.size() != count) {
                if (begin + records.size() < MigrationIDGen.count) {
                    System.out.println("ids records.size() != count size= " + records.size() + " , sql : " + sql);
                }
            }
            return records;
        } finally {
            if (conn != null && conn.isClosed() == false) {
                conn.close();
            }
        }
    }

    public void init() throws Exception {
        long st = System.currentTimeMillis();
        count = getRecordsNumber();
        if (count < 0) {
            throw new Exception("ids getRecordsNumber() error ");
        }
        if (count < 1) {
            System.out.println("ids records count == 0");
            return;
        }
        System.out.println("...... ids records count == " + count);
        int nw = 50;
        long one = (count + (nw - 1)) / nw;
        CountDownLatch runningThreadNum = null;
        if (one < 1) {
            runningThreadNum = new CountDownLatch(1);
            Migration m = new Migration(runningThreadNum);
            m.setBegin(0);
            m.setCount(count);
            m.start();
        } else {
            runningThreadNum = new CountDownLatch(nw);
            for (int i = 0; i < nw; i++) {
                Migration m = new Migration(runningThreadNum);
                m.setBegin(one * i);
                m.setCount(one);
                m.start();
            }
        }
        runningThreadNum.await();
        long et = System.currentTimeMillis();
        System.out.println("...... ids migration finished time ms = " + (et - st));
    }

    private class Migration extends Thread {

        private CountDownLatch runningThreadNum;

        public Migration(CountDownLatch runningThreadNum) {
            this.runningThreadNum = runningThreadNum;
        }

        long begin = 0;
        long count = 0;

        public long getBegin() {
            return begin;
        }

        public void setBegin(long begin) {
            this.begin = begin;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        public void run() {
            long s = begin;
            long c = count;
            String threadid = Thread.currentThread().getName() + " --- ";
            long st = System.currentTimeMillis();
            System.out.println(threadid + "ids thread begin = " + begin + ", count = " + count);
            while (c > 0) {
                String key = "";
                long n = c > 10000 ? 10000 : c;
                try {
                    int num = 0;
                    long ts = System.currentTimeMillis();
                    List<key_val> records = fetchRecords((int) s, (int) n);
                    for (key_val elt : records) {
                        key = elt.key;
                        {
                            int hash = DefaultHashGenerator.hash(key);
                            if (!PigeonMigration.rightRange(hash)) {
                                System.out.println(threadid + " ?????? ids key bad hash range , key = " + key + ", hash = " + hash);
                                continue;
                            }
                        }
                        long val = Long.valueOf(elt.val).longValue();
                        while (true) {
                            long id = pigeonStoreEngine.getIdGenerator().getId(key);
                            if (id > val) {
                                break;
                            }
                        }
                        ++num;
                    }
                    long te = System.currentTimeMillis();
                    System.out.println(threadid + " idserver finished " + num + " cost time ms = " + (te - ts));
                    if (records.size() != n) {
                        if (c - records.size() != 0) {
                            if (s + records.size() < MigrationIDGen.count) {
                                System.out.println(threadid + " !!!!!! ids panic ....... delta = " + (c - records.size()) + ", n = " + n + ", c = " + c + ", records = " + records.size());
                            }
                        }
                        break;
                    }
                    s += n;
                    c -= n;
                    System.out.println(threadid + "ids finish = " + n + ", left = " + c);
                } catch (Exception e) {
                    e.printStackTrace();
                    if (key.length() > 0) {
                        System.out.println(threadid + " !!!!!! ids panic ...... key : " + key + " error ");
                    }
                }
            }
            long et = System.currentTimeMillis();
            System.out.println(threadid + "ids one thread finished time ms = " + (et - st) + " , begin = " + begin + " , count = " + count);
            runningThreadNum.countDown();
        }
    }
}



