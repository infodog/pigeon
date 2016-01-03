package net.xinshi.pigeon.updateflexobject;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.adapter.impl.DistributedPigeonEngine;
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
 * Date: 12-4-16
 * Time: 下午3:43
 * To change this template use File | Settings | File Templates.
 */

public class PigeonUpdateFlexObject {
    static String dbUrl;
    static String dbUserName;
    static String dbPassword;
    static String oldString;
    static String newString;
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
        oldString = jo.optString("oldString");
        if (oldString.indexOf("'") >= 0) {
            throw new Exception("bad oldString : " + oldString);
        }
        newString = jo.optString("newString");
        pigeonStoreConfigFile = jo.optString("pigeonStoreConfigFile");
        if (dbUrl.length() < 1 || dbUserName.length() < 1 || dbPassword.length() < 1
                || oldString.length() < 1 || newString.length() < 1 || pigeonStoreConfigFile.length() < 1) {
            throw new Exception("config file bad ... ");
        }
        System.out.println("dbUrl = " + dbUrl);
        System.out.println("dbUserName = " + dbUserName);
        System.out.println("dbPassword = " + dbPassword);
        System.out.println("oldString = " + oldString);
        System.out.println("newString = " + newString);
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
        PigeonUpdateFlexObject.ds = new TransactionAwareDataSourceProxy(lowlevelds);
        txManager = new DataSourceTransactionManager();
        txManager.setDataSource(PigeonUpdateFlexObject.ds);
        return PigeonUpdateFlexObject.ds;
    }

    static class UpdateFlexObjectThread extends Thread {
        private CountDownLatch runningThreadNum;

        public UpdateFlexObjectThread(CountDownLatch runningThreadNum) {
            this.runningThreadNum = runningThreadNum;
        }

        public void run() {
            try {
                UpdateFlexObject ufo = new UpdateFlexObject(PigeonUpdateFlexObject.ds, PigeonUpdateFlexObject.pigeonStoreEngine);
                ufo.setOldString(PigeonUpdateFlexObject.oldString);
                ufo.setNewString(PigeonUpdateFlexObject.newString);
                ufo.init();
            } catch (Exception e) {
                e.printStackTrace();
            }
            runningThreadNum.countDown();
        }
    }

    public static void main(String[] args) throws Exception {
        long st = System.currentTimeMillis();

        if (args.length != 1) {
            System.out.println("useage : java PigeonUpdateFlexObject PigeonUpdateFlexObject.conf");
            return;
        }
        String config = args[0];

        // String config = "pigeon-tools/resources/pigeonupdate.conf";

        init(config);

        final CountDownLatch runningThreadNum = new CountDownLatch(1);

        UpdateFlexObjectThread ufot = new UpdateFlexObjectThread(runningThreadNum);
        ufot.start();

        runningThreadNum.await();

        long et = System.currentTimeMillis();

        System.out.println("PigeonUpdateFlexObject all ...... time ms = " + (et - st));

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
