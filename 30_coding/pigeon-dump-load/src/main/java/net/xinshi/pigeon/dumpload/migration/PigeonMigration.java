package net.xinshi.pigeon.dumpload.migration;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.adapter.impl.DistributedPigeonEngine;
import net.xinshi.pigeon.backup.PigeonBackup;
import net.xinshi.pigeon.distributed.util.DefaultHashGenerator;
import net.xinshi.pigeon.dumpload.dumpdb.PigeonDump;
import net.xinshi.pigeon.dumpload.loaddata.PigeonLoad;
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
 * Date: 12-4-11
 * Time: 上午11:13
 * To change this template use File | Settings | File Templates.
 */

public class PigeonMigration {
    public static boolean scan = false;
    static String dbUrl;
    static String dbUserName;
    static String dbPassword;
    static String listTable = "t_listband";
    static int leftBoundary = -2147483648;
    static int rightBoundary = 2147483647;
    static String pigeonStoreConfigFile;
    static DataSource ds;
    static DataSourceTransactionManager txManager;
    static IPigeonStoreEngine pigeonStoreEngine;

    public static void init(String config) throws Exception {
        File f = new File(config);
        FileInputStream is = new FileInputStream(f);
        byte[] b = new byte[(int) f.length()];
        is.read(b);
        is.close();
        String s = new String(b, "UTF-8");
        String info = StringUtils.trim(s);
        JSONObject jo = new JSONObject(info);
        dbUrl = jo.optString("dbUrl");
        dbUserName = jo.optString("dbUserName");
        dbPassword = jo.optString("dbPassword");
        if (jo.optString("listTable").length() > 0) {
            listTable = jo.optString("listTable");
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
        pigeonStoreConfigFile = jo.optString("pigeonStoreConfigFile");
        if (dbUrl.length() < 1 || dbUserName.length() < 1 || dbPassword.length() < 1 || pigeonStoreConfigFile.length() < 1) {
            throw new Exception("config file bad ... ");
        }
        System.out.println("dbUrl = " + dbUrl);
        System.out.println("dbUserName = " + dbUserName);
        System.out.println("dbPassword = " + dbPassword);
        System.out.println("listTable = " + listTable);
        System.out.println("leftBoundary = " + leftBoundary + ", rightBoundary = " + rightBoundary);
        System.out.println("pigeonStoreConfigFile = " + pigeonStoreConfigFile);
        createDs();
        is.close();
        f = new File(pigeonStoreConfigFile);
        pigeonStoreEngine = new DistributedPigeonEngine(f.getAbsolutePath());
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
        PigeonMigration.ds = new TransactionAwareDataSourceProxy(lowlevelds);
        txManager = new DataSourceTransactionManager();
        txManager.setDataSource(PigeonMigration.ds);
        return PigeonMigration.ds;
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

    static class MigrationAtomThread extends Thread {
        private CountDownLatch runningThreadNum;

        public MigrationAtomThread(CountDownLatch runningThreadNum) {
            this.runningThreadNum = runningThreadNum;
        }

        public void run() {
            try {
                MigrationAtom ma = new MigrationAtom(PigeonMigration.ds, PigeonMigration.pigeonStoreEngine);
                ma.init();
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
                MigrationList ml = new MigrationList(PigeonMigration.ds, PigeonMigration.pigeonStoreEngine, PigeonMigration.listTable);
                ml.init();
            } catch (Exception e) {
                e.printStackTrace();
            }
            runningThreadNum.countDown();
        }
    }

    static class MigrationFlexObjectThread extends Thread {
        private CountDownLatch runningThreadNum;

        public MigrationFlexObjectThread(CountDownLatch runningThreadNum) {
            this.runningThreadNum = runningThreadNum;
        }

        public void run() {
            try {
                MigrationFlexObject mfo = new MigrationFlexObject(PigeonMigration.ds, PigeonMigration.pigeonStoreEngine);
                mfo.init();
            } catch (Exception e) {
                e.printStackTrace();
            }
            runningThreadNum.countDown();
        }
    }

    static class MigrationIDGenThread extends Thread {
        private CountDownLatch runningThreadNum;

        public MigrationIDGenThread(CountDownLatch runningThreadNum) {
            this.runningThreadNum = runningThreadNum;
        }

        public void run() {
            try {
                MigrationIDGen midg = new MigrationIDGen(PigeonMigration.ds, PigeonMigration.pigeonStoreEngine);
                midg.init();
            } catch (Exception e) {
                e.printStackTrace();
            }
            runningThreadNum.countDown();
        }
    }

    public static void main0(String[] args) throws Exception {
        long st = System.currentTimeMillis();

        if (args.length != 1) {
            System.out.println("useage : java PigeonMigration pigeonmigration.conf");
            return;
        }
        String config = args[0];

        // String config = "pigeon-tools/resources/pigeonmigration.conf";

        init(config);

        final CountDownLatch runningThreadNum = new CountDownLatch(4);

        MigrationFlexObjectThread mfot = new MigrationFlexObjectThread(runningThreadNum);
        mfot.start();

        MigrationListThread mlt = new MigrationListThread(runningThreadNum);
        mlt.start();

        MigrationAtomThread mat = new MigrationAtomThread(runningThreadNum);
        mat.start();

        MigrationIDGenThread midgt = new MigrationIDGenThread(runningThreadNum);
        midgt.start();

        runningThreadNum.await();

        long et = System.currentTimeMillis();

        System.out.println("PigeonMigration all ...... time ms = " + (et - st));
    }

    public static void main(String[] args) throws Exception {
        long st = System.currentTimeMillis();

        if (args.length < 1) {
            System.out.println("useage : java PigeonMigration cmd config1 config2");
            return;
        }

        String cmd = args[0];
        if (args[0].compareToIgnoreCase("backup") == 0) {
            String mid = null;
            if (args.length == 2) {
                mid = args[1];
            }
            File f = new File("../conf/pigeonnodes.conf");
            IPigeonStoreEngine pigeonStoreEngine = new DistributedPigeonEngine(f.getAbsolutePath());
            PigeonBackup.backup(mid, "../data", pigeonStoreEngine);
        }
        if (cmd.compareToIgnoreCase("scan") == 0) {
            if (args.length < 2) {
                System.out.println("useage : java PigeonMigration load pigeonload.conf");
                return;
            }
            scan = true;
            PigeonLoad.doit(args[1]);
        }
        if (cmd.compareToIgnoreCase("dump") == 0) {
            if (args.length < 2) {
                System.out.println("useage : java PigeonMigration dump pigeondump.conf");
                return;
            }
            PigeonDump.doit(args[1]);
        }
        if (cmd.compareToIgnoreCase("load") == 0) {
            if (args.length < 2) {
                System.out.println("useage : java PigeonMigration load pigeonload.conf");
                return;
            }
            PigeonLoad.doit(args[1]);
        }
        if (cmd.compareToIgnoreCase("dumpload") == 0) {
            if (args.length < 3) {
                System.out.println("useage : java PigeonMigration dumpload pigeondump.conf pigeonload.conf");
                return;
            }
            PigeonDump.doit(args[1]);
            PigeonLoad.doit(args[2]);
        }

        long et = System.currentTimeMillis();
        System.out.println("PigeonMigration all ...... time ms = " + (et - st));

        try {
            while (true) {
                System.exit(0);
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
