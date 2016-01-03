package net.xinshi.pigeon.dumpload.dumpdb;

import net.xinshi.pigeon.distributed.util.DefaultHashGenerator;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.util.concurrent.CountDownLatch;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-5-10
 * Time: 上午10:34
 * To change this template use File | Settings | File Templates.
 */

public class PigeonDump {
    static String flexobjectVersion;
    static String dbUrl;
    static String dbUserName;
    static String dbPassword;
    static String flexobjectTable = "t_flexobject";
    static String listTable = "t_listband";
    static String atomTable = "t_simpleatom";
    static String idserverTable = "t_ids";
    static String flexobjectFile = "./data/flexobject.txt";
    static String listFile = "./data/list.txt";
    static String atomFile = "./data/atom.txt";
    static String idserverFile = "./data/idserver.txt";
    static int leftBoundary = -2147483648;
    static int rightBoundary = 2147483647;
    static DataSource ds;
    static DataSourceTransactionManager txManager;

    public static void init(String config) throws Exception {
        File f = new File(config);
        FileInputStream is = new FileInputStream(f);
        byte[] b = new byte[(int) f.length()];
        is.read(b);
        is.close();
        String s = new String(b, "UTF-8");
        String info = StringUtils.trim(s);
        JSONObject jo = new JSONObject(info);
        flexobjectVersion = jo.optString("flexobjectVersion");
        if (flexobjectVersion.length() == 0) {
            flexobjectVersion = "2";
        }
        dbUrl = jo.optString("dbUrl");
        dbUserName = jo.optString("dbUserName");
        dbPassword = jo.optString("dbPassword");
        JSONObject jso = null;
        jso = jo.optJSONObject("flexobject");
        if (jso != null) {
            flexobjectTable = jso.optString("tableName");
            flexobjectFile = jso.optString("saveToFile");
            if (flexobjectTable.length() < 1 || flexobjectFile.length() < 1) {
                throw new Exception("flexobject config bad");
            }
            File ft = new File(flexobjectFile);
            if (ft.exists()) {
                throw new Exception("flexobject saveToFile exists");
            }
            flexobjectFile = ft.getAbsolutePath().replace("\\", "/");
        }
        jso = jo.optJSONObject("list");
        if (jso != null) {
            listTable = jso.optString("tableName");
            listFile = jso.optString("saveToFile");
            if (listTable.length() < 1 || listFile.length() < 1) {
                throw new Exception("list config bad");
            }
            File ft = new File(listFile);
            if (ft.exists()) {
                throw new Exception("list saveToFile exists");
            }
            listFile = ft.getAbsolutePath().replace("\\", "/");
        }
        jso = jo.optJSONObject("atom");
        if (jso != null) {
            atomTable = jso.optString("tableName");
            atomFile = jso.optString("saveToFile");
            if (atomTable.length() < 1 || atomFile.length() < 1) {
                throw new Exception("atom config bad");
            }
            File ft = new File(atomFile);
            if (ft.exists()) {
                throw new Exception("atom saveToFile exists");
            }
            atomFile = ft.getAbsolutePath().replace("\\", "/");
        }
        jso = jo.optJSONObject("idserver");
        if (jso != null) {
            idserverTable = jso.optString("tableName");
            idserverFile = jso.optString("saveToFile");
            if (idserverTable.length() < 1 || idserverFile.length() < 1) {
                throw new Exception("idserver config bad");
            }
            File ft = new File(idserverFile);
            if (ft.exists()) {
                throw new Exception("idserver saveToFile exists");
            }
            idserverFile = ft.getAbsolutePath().replace("\\", "/");
        }
        {
            String range = jo.optString("range");
            if (range.length() > 0) {
                String[] parts = range.split("~");
                if (parts.length == 2) {
                    leftBoundary = Integer.parseInt(parts[0]);
                    rightBoundary = Integer.parseInt(parts[1]);
                }
            }
        }
        if (dbUrl.length() < 1 || dbUserName.length() < 1 || dbPassword.length() < 1) {
            throw new Exception("config file bad ... ");
        }
        System.out.println("flexobjectVersion = " + flexobjectVersion);
        System.out.println("dbUrl = " + dbUrl);
        System.out.println("dbUserName = " + dbUserName);
        System.out.println("dbPassword = " + dbPassword);
        System.out.println("leftBoundary = " + leftBoundary + ", rightBoundary = " + rightBoundary + "\n");
        System.out.println("flexobject tableName = " + flexobjectTable + ", saveToFile = " + flexobjectFile);
        System.out.println("list tableName = " + listTable + ", saveToFile = " + listFile);
        System.out.println("atom tableName = " + atomTable + ", saveToFile = " + atomFile);
        System.out.println("idserver tableName = " + idserverTable + ", saveToFile = " + idserverFile);
        createDs();
        is.close();
    }

    protected static DataSource createDs() throws Exception {
        BasicDataSource lowlevelds = new BasicDataSource();
        String driverClass = "org.gjt.mm.mysql.Driver";
        lowlevelds.setUrl(dbUrl);
        lowlevelds.setUsername(dbUserName);
        lowlevelds.setPassword(dbPassword);
        lowlevelds.addConnectionProperty("socketTimeout", "1200000");
        lowlevelds.setPoolPreparedStatements(true);
        lowlevelds.setMaxActive(10);
        lowlevelds.setDefaultAutoCommit(false);
        lowlevelds.setValidationQuery("select count(*) from t_testwhileidle");
        lowlevelds.setTestOnBorrow(true);
        lowlevelds.setTestOnReturn(true);
        lowlevelds.setTestWhileIdle(true);
        lowlevelds.setTimeBetweenEvictionRunsMillis(1200000);
        PigeonDump.ds = new TransactionAwareDataSourceProxy(lowlevelds);
        txManager = new DataSourceTransactionManager();
        txManager.setDataSource(PigeonDump.ds);
        return PigeonDump.ds;
    }

    public static boolean rightRange(int hash) {
        if (hash >= leftBoundary && hash <= rightBoundary) {
            return true;
        }
        return false;
    }

    public static boolean rightRange(String key) {
        int hash = DefaultHashGenerator.hash(key);
        return rightRange(hash);
    }

    static class MigrationFlexObjectThread extends Thread {
        private CountDownLatch runningThreadNum;

        public MigrationFlexObjectThread(CountDownLatch runningThreadNum) {
            this.runningThreadNum = runningThreadNum;
        }

        public void run() {
            try {
                if (flexobjectVersion.equals("3")) {
                    DumpFlexObjectV3 dfo = new DumpFlexObjectV3(ds, flexobjectTable, flexobjectFile);
                    dfo.dump();
                } else {
                    DumpFlexObject dfo = new DumpFlexObject(ds, flexobjectTable, flexobjectFile);
                    dfo.dump();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            runningThreadNum.countDown();
        }
    }

    static class MigrationListThread extends Thread {
        private CountDownLatch runningThreadNum;

        public MigrationListThread(CountDownLatch runningThreadNum) {
            this.runningThreadNum = runningThreadNum;
        }

        public void run() {
            try {
                DumpList dl = new DumpList(ds, listTable, listFile);
                dl.dump();
            } catch (Exception e) {
                e.printStackTrace();
            }
            runningThreadNum.countDown();
        }
    }

    static class MigrationAtomThread extends Thread {
        private CountDownLatch runningThreadNum;

        public MigrationAtomThread(CountDownLatch runningThreadNum) {
            this.runningThreadNum = runningThreadNum;
        }

        public void run() {
            try {
                DumpAtom da = new DumpAtom(ds, atomTable, atomFile);
                da.dump();
            } catch (Exception e) {
                e.printStackTrace();
            }
            runningThreadNum.countDown();
        }
    }

    static class MigrationIdServerThread extends Thread {
        private CountDownLatch runningThreadNum;

        public MigrationIdServerThread(CountDownLatch runningThreadNum) {
            this.runningThreadNum = runningThreadNum;
        }

        public void run() {
            try {
                DumpIdServer dis = new DumpIdServer(ds, idserverTable, idserverFile);
                dis.dump();
            } catch (Exception e) {
                e.printStackTrace();
            }
            runningThreadNum.countDown();
        }
    }

    public static void doit(String config) throws Exception {
        long st = System.currentTimeMillis();

        init(config);

        final CountDownLatch runningThreadNum = new CountDownLatch(4);

        MigrationFlexObjectThread mfot = new MigrationFlexObjectThread(runningThreadNum);
        mfot.start();

        MigrationListThread mlt = new MigrationListThread(runningThreadNum);
        mlt.start();

        MigrationAtomThread mat = new MigrationAtomThread(runningThreadNum);
        mat.start();

        MigrationIdServerThread midgt = new MigrationIdServerThread(runningThreadNum);
        midgt.start();

        runningThreadNum.await();

        long et = System.currentTimeMillis();

        System.out.println("PigeonDump all ...... time ms = " + (et - st));
    }

    public static void main(String[] args) throws Exception {
        String config = "pigeon-tools/resources/Pigeondump.conf";
        if (args.length > 0) {
            config = args[0];
        }
        doit(config);
    }

}

