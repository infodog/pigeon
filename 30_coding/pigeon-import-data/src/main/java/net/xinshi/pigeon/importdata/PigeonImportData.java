package net.xinshi.pigeon.importdata;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.adapter.impl.DistributedPigeonEngine;
import net.xinshi.pigeon.util.TimeTools;
import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.util.concurrent.CountDownLatch;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-6-8
 * Time: 下午3:22
 * To change this template use File | Settings | File Templates.
 */

public class PigeonImportData {
    static String pigeonStoreConfigFile;
    static IPigeonStoreEngine pigeonStoreEngine;

    static config configFlexObject = new config();
    static config configList = new config();
    static config configAtom = new config();
    static config configIdServer = new config();

    public static void init(String config) throws Exception {
        File directory = new File(".");
        System.out.println("current dir = \"" + directory.getAbsolutePath() + "\"");
        File f = new File(config);
        FileInputStream is = new FileInputStream(f.getAbsoluteFile());
        byte[] b = new byte[(int) f.length()];
        is.read(b);
        is.close();
        String s = new String(b, "UTF-8");
        String info = StringUtils.trim(s);
        JSONObject jo = new JSONObject(info);
        pigeonStoreConfigFile = jo.optString("pigeonStoreConfigFile");
        if (pigeonStoreConfigFile.length() < 1) {
            throw new Exception("config file bad ... ");
        }
        JSONObject jso = null;
        jso = jo.optJSONObject("flexobject");
        configFlexObject.DataDir = (new File(directory.getAbsolutePath() + "/" + jso.optString("DataDir"))).getAbsolutePath();
        configFlexObject.beginVersion = Long.valueOf(jso.optString("beginVersion")).longValue();
        if (configFlexObject.beginVersion < 0) {
            configFlexObject.beginVersion = 0;
        }
        configFlexObject.endVersion = Long.valueOf(jso.optString("endVersion")).longValue();
        System.out.println("configFlexObject.DataDir = " + configFlexObject.DataDir);
        System.out.println("configFlexObject.beginVersion = " + configFlexObject.beginVersion);
        System.out.println("configFlexObject.endVersion = " + configFlexObject.endVersion);
        jso = jo.optJSONObject("list");
        configList.DataDir = (new File(directory.getAbsolutePath() + "/" + jso.optString("DataDir"))).getAbsolutePath();
        configList.beginVersion = Long.valueOf(jso.optString("beginVersion")).longValue();
        if (configList.beginVersion < 0) {
            configList.beginVersion = 0;
        }
        configList.endVersion = Long.valueOf(jso.optString("endVersion")).longValue();
        System.out.println("configList.DataDir = " + configList.DataDir);
        System.out.println("configList.beginVersion = " + configList.beginVersion);
        System.out.println("configList.endVersion = " + configList.endVersion);
        jso = jo.optJSONObject("atom");
        configAtom.DataDir = (new File(directory.getAbsolutePath() + "/" + jso.optString("DataDir"))).getAbsolutePath();
        configAtom.beginVersion = Long.valueOf(jso.optString("beginVersion")).longValue();
        if (configAtom.beginVersion < 0) {
            configAtom.beginVersion = 0;
        }
        configAtom.endVersion = Long.valueOf(jso.optString("endVersion")).longValue();
        System.out.println("configAtom.DataDir = " + configAtom.DataDir);
        System.out.println("configAtom.beginVersion = " + configAtom.beginVersion);
        System.out.println("configAtom.endVersion = " + configAtom.endVersion);
        jso = jo.optJSONObject("idserver");
        configIdServer.DataDir = (new File(directory.getAbsolutePath() + "/" + jso.optString("DataDir"))).getAbsolutePath();
        configIdServer.beginVersion = Long.valueOf(jso.optString("beginVersion")).longValue();
        if (configIdServer.beginVersion < 0) {
            configIdServer.beginVersion = 0;
        }
        configIdServer.endVersion = Long.valueOf(jso.optString("endVersion")).longValue();
        System.out.println("configIdServer.DataDir = " + configIdServer.DataDir);
        System.out.println("configIdServer.beginVersion = " + configIdServer.beginVersion);
        System.out.println("configIdServer.endVersion = " + configIdServer.endVersion);
        // System.out.println("pigeonStoreConfigFile = " + pigeonStoreConfigFile);
        is.close();
        f = new File(pigeonStoreConfigFile);
        System.out.println("pigeonStoreConfigFile = " + f.getAbsolutePath());
        pigeonStoreEngine = new DistributedPigeonEngine(f.getAbsolutePath());
    }

    static class ImportFlexObjectThread extends Thread {
        private CountDownLatch runningThreadNum;

        public ImportFlexObjectThread(CountDownLatch runningThreadNum) {
            this.runningThreadNum = runningThreadNum;
        }

        public void run() {
            ImportFlexObject ifo = new ImportFlexObject(configFlexObject, pigeonStoreEngine);
            try {
                long ts = System.currentTimeMillis();
                ifo.init();
                long n = ifo.ImportDataFiles();
                long te = System.currentTimeMillis();
                System.out.println(TimeTools.getNowTimeString() + " Import FlexObject finished ms = " + (te - ts) + ", n = " + n);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                System.out.println(TimeTools.getNowTimeString() + " Import FlexObject lastVersion = " + ifo.lastVersion);
            }
            runningThreadNum.countDown();
        }
    }

    static class ImportAtomThread extends Thread {
        private CountDownLatch runningThreadNum;

        public ImportAtomThread(CountDownLatch runningThreadNum) {
            this.runningThreadNum = runningThreadNum;
        }

        public void run() {
            ImportAtom ifo = new ImportAtom(configAtom, pigeonStoreEngine);
            try {
                long ts = System.currentTimeMillis();
                ifo.init();
                long n = ifo.ImportDataFiles();
                long te = System.currentTimeMillis();
                System.out.println(TimeTools.getNowTimeString() + " Import Atom finished ms = " + (te - ts) + ", n = " + n);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                System.out.println(TimeTools.getNowTimeString() + " Import Atom lastVersion = " + ifo.lastVersion);
            }
            runningThreadNum.countDown();
        }
    }

    static class ImportIdServerThread extends Thread {
        private CountDownLatch runningThreadNum;

        public ImportIdServerThread(CountDownLatch runningThreadNum) {
            this.runningThreadNum = runningThreadNum;
        }

        public void run() {
            ImportIdServer ifo = new ImportIdServer(configIdServer, pigeonStoreEngine);
            try {
                long ts = System.currentTimeMillis();
                ifo.init();
                long n = ifo.ImportDataFiles();
                long te = System.currentTimeMillis();
                System.out.println(TimeTools.getNowTimeString() + " Import IdServer finished ms = " + (te - ts) + ", n = " + n);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                System.out.println(TimeTools.getNowTimeString() + " Import IdServer lastVersion = " + ifo.lastVersion);
            }
            runningThreadNum.countDown();
        }
    }

    static class ImportListThread extends Thread {
        private CountDownLatch runningThreadNum;

        public ImportListThread(CountDownLatch runningThreadNum) {
            this.runningThreadNum = runningThreadNum;
        }

        public void run() {
            ImportList ifo = new ImportList(configList, pigeonStoreEngine);
            try {
                long ts = System.currentTimeMillis();
                ifo.init();
                long n = ifo.ImportDataFiles();
                long te = System.currentTimeMillis();
                System.out.println(TimeTools.getNowTimeString() + " Import List finished ms = " + (te - ts) + ", n = " + n);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                System.out.println(TimeTools.getNowTimeString() + " Import List lastVersion = " + ifo.lastVersion);
            }
            runningThreadNum.countDown();
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.out.println("useage : java PigeonImportData pigeonimportdata.conf");
            return;
        }
        String config = args[0];

        // String config = "pigeon-import-data/resources/pigeonimportdata.conf";

        init(config);

        long st = System.currentTimeMillis();

        final CountDownLatch runningThreadNum = new CountDownLatch(4);

        ImportFlexObjectThread ifot = new ImportFlexObjectThread(runningThreadNum);
        ifot.start();

        ImportAtomThread iat = new ImportAtomThread(runningThreadNum);
        iat.start();

        ImportIdServerThread iist = new ImportIdServerThread(runningThreadNum);
        iist.start();

        ImportListThread ilt = new ImportListThread(runningThreadNum);
        ilt.start();

        runningThreadNum.await();

        long et = System.currentTimeMillis();

        System.out.println("PigeonImportData all ...... time ms = " + (et - st));

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
