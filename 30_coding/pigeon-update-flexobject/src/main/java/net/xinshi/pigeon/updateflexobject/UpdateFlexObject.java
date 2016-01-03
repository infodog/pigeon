package net.xinshi.pigeon.updateflexobject;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-4-16
 * Time: 下午3:59
 * To change this template use File | Settings | File Templates.
 */

public class UpdateFlexObject {
    static long count = -1;
    String oldString;
    String newString;
    DataSource ds;
    IPigeonStoreEngine pigeonStoreEngine;

    public UpdateFlexObject(DataSource ds, IPigeonStoreEngine pigeonStoreEngine) {
        this.ds = ds;
        this.pigeonStoreEngine = pigeonStoreEngine;
    }

    public String getOldString() {
        return oldString;
    }

    public void setOldString(String oldString) {
        this.oldString = oldString;
    }

    public String getNewString() {
        return newString;
    }

    public void setNewString(String newString) {
        this.newString = newString;
    }

    long getRecordsNumber() throws Exception {
        System.out.println("flexobject update do getRecordsNumber() ... ");
        long count = -1;
        String sql = "select count(*) as count from t_flexobject where (isCompressed=0 and content like '%" + oldString + "%') or isCompressed=1";
        System.out.println("FlexObject alter sql : " + sql);
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

    Map fetchRecords(int begin, int count) throws Exception {
        HashMap<String, String> records = new HashMap<String, String>();
        String sql = "select name from t_flexobject where (isCompressed=0 and content like '%" + oldString + "%') or isCompressed=1 order by name limit " + begin + "," + count;
        System.out.println("fetchRecords sql : " + sql);
        Connection conn = null;
        try {
            conn = ds.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                byte[] bytes = rs.getBytes("name");
                String name = null;
                if (bytes != null) {
                    name = new String(bytes, "UTF-8");
                }
                records.put(name, name);
            }
            rs.close();
            stmt.close();
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
            throw new Exception("FlexObject getRecordsNumber() error ");
        }
        if (count < 1) {
            System.out.println("flexobject records count == 0");
            return;
        }
        System.out.println("...... flexobject records count == " + count);
        long one = (count + 9) / 10;
        CountDownLatch runningThreadNum = null;
        if (one < 1) {
            runningThreadNum = new CountDownLatch(1);
            Update u = new Update(runningThreadNum);
            u.setBegin(0);
            u.setCount(count);
            u.start();
        } else {
            runningThreadNum = new CountDownLatch(10);
            for (int i = 0; i < 10; i++) {
                Update u = new Update(runningThreadNum);
                u.setBegin(one * i);
                u.setCount(one);
                u.start();
            }
        }
        runningThreadNum.await();
        long et = System.currentTimeMillis();
        System.out.println("...... flexobject update finished time ms = " + (et - st));
    }

    private class Update extends Thread {

        private CountDownLatch runningThreadNum;

        public Update(CountDownLatch runningThreadNum) {
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
            System.out.println(threadid + "flexobject thread begin = " + begin + ", count = " + count);
            while (c > 0) {
                String key = "";
                long n = c > 10000 ? 10000 : c;
                try {
                    Map<String, String> records = fetchRecords((int) s, (int) n);
                    for (Map.Entry<String, String> elt : records.entrySet()) {
                        key = elt.getKey();
                        if (key == null) {
                            continue;
                        }
                        try {
                            String content = pigeonStoreEngine.getFlexObjectFactory().getContent(key);
                            if (content.indexOf(oldString) >= 0) {
                                System.out.println("update name = " + key);
                                content = content.replace(oldString, newString);
                                pigeonStoreEngine.getFlexObjectFactory().saveContent(key, content);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.out.println(e.getMessage());
                        }
                    }
                    if (records.size() != n) {
                        if (c - records.size() != 0) {
                            if (s + records.size() < UpdateFlexObject.count) {
                                System.out.println(threadid + " !!!!!! flexobject panic ....... delta = " + (c - records.size()));
                            }
                        }
                        break;
                    }
                    s += n;
                    c -= n;
                    System.out.println(threadid + "flexobject finish = " + n + ", left = " + c);
                } catch (Exception e) {
                    e.printStackTrace();
                    if (key.length() > 0) {
                        System.out.println(threadid + " !!!!!! flexobject panic ...... key : " + key + " error ");
                    }
                }
            }
            long et = System.currentTimeMillis();
            System.out.println(threadid + "flexobject one thread finished time ms = " + (et - st) + " , begin = " + begin + " , count = " + count);
            runningThreadNum.countDown();
        }
    }
}

