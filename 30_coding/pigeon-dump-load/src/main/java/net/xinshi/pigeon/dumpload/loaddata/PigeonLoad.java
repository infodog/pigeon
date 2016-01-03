package net.xinshi.pigeon.dumpload.loaddata;

import net.xinshi.pigeon.adapter.IPigeonEngine;
import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.adapter.impl.DistributedPigeonEngine;
import net.xinshi.pigeon.adapter.impl.NormalPigeonEngine;
import net.xinshi.pigeon.saas.SaasPigeonEngine;
import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-5-11
 * Time: 下午4:54
 * To change this template use File | Settings | File Templates.
 */

public class PigeonLoad {
    static String flexobjectVersion;
    static String pigeonStoreConfigFile;
    static IPigeonStoreEngine pigeonStoreEngine;
    static String saasId;
    static Vector<String> flexobjectFiles = new Vector<String>();
    static Vector<String> listFiles = new Vector<String>();
    static Vector<String> atomFiles = new Vector<String>();
    static Vector<String> idserverFiles = new Vector<String>();
    static Logger logger = Logger.getLogger(PigeonLoad.class.getName());



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
            flexobjectVersion = "3";
        }
        pigeonStoreConfigFile = jo.optString("pigeonStoreConfigFile");
        if (pigeonStoreConfigFile.length() < 1) {
            throw new Exception("config file bad ... ");
        }
        JSONArray jsa = null;
        jsa = jo.getJSONArray("flexobject");
        if (jsa != null) {
            for (int i = 0; i < jsa.length(); i++) {
                JSONObject jot = jsa.getJSONObject(i);
                String fileName = (String) jot.getObjectMap().values().toArray()[0];
                flexobjectFiles.add(fileName);
                System.out.println("flexobjectFiles : " + fileName);
            }
        }

        jsa = jo.getJSONArray("list");
        if (jsa != null) {
            for (int i = 0; i < jsa.length(); i++) {
                JSONObject jot = jsa.getJSONObject(i);
                String fileName = (String) jot.getObjectMap().values().toArray()[0];
                listFiles.add(fileName);
                System.out.println("listFiles : " + fileName);
            }
        }

        jsa = jo.getJSONArray("atom");
        if (jsa != null) {
            for (int i = 0; i < jsa.length(); i++) {
                JSONObject jot = jsa.getJSONObject(i);
                String fileName = (String) jot.getObjectMap().values().toArray()[0];
                atomFiles.add(fileName);
                System.out.println("atomFiles : " + fileName);
            }
        }

        jsa = jo.getJSONArray("idserver");
        if (jsa != null) {
            for (int i = 0; i < jsa.length(); i++) {
                JSONObject jot = jsa.getJSONObject(i);
                String fileName = (String) jot.getObjectMap().values().toArray()[0];
                idserverFiles.add(fileName);
                System.out.println("idserverFiles : " + fileName);
            }
        }
        System.out.println("flexobjectVersion = " + flexobjectVersion);
        System.out.println("pigeonStoreConfigFile = " + pigeonStoreConfigFile);
//        is.close();
        f = new File(pigeonStoreConfigFile);
        saasId = jo.optString("saasId");

        logger.info("going to init pigeon, config file=" + f.getAbsolutePath());
        IPigeonStoreEngine rawPigeon = new DistributedPigeonEngine(f.getAbsolutePath());
//        pigeonStoreEngine = new DistributedPigeonEngine(f.getAbsolutePath());

        if(StringUtils.isNotBlank(saasId)){
            pigeonStoreEngine = new SaasPigeonEngine();
            NormalPigeonEngine normalPigeonEngine = new NormalPigeonEngine();
            normalPigeonEngine.setPigeonStoreEngine(rawPigeon);
            ((SaasPigeonEngine)(pigeonStoreEngine)).setRawPigeonEngine(normalPigeonEngine);

        }
        else{
            pigeonStoreEngine = rawPigeon;
        }
    }

    static class LoadAtomThread extends Thread {
        private CountDownLatch runningThreadNum;

        public LoadAtomThread(CountDownLatch runningThreadNum) {
            this.runningThreadNum = runningThreadNum;
        }

        public void run() {
            try {
                if(StringUtils.isNotBlank(PigeonLoad.saasId)){
                    ((SaasPigeonEngine)(pigeonStoreEngine)).setCurrentMerchantId(PigeonLoad.saasId);
                }
                LoadAtom ma = new LoadAtom(pigeonStoreEngine, atomFiles);
                ma.init();
            } catch (Exception e) {
                e.printStackTrace();
            }
            runningThreadNum.countDown();
        }
    }

    static class LoadFlexObjectThread extends Thread {
        private CountDownLatch runningThreadNum;

        public LoadFlexObjectThread(CountDownLatch runningThreadNum) {
            this.runningThreadNum = runningThreadNum;
        }

        public void run() {
            try {
                if(StringUtils.isNotBlank(saasId)){
                    ((SaasPigeonEngine)(pigeonStoreEngine)).setCurrentMerchantId(saasId);
                }
                if (flexobjectVersion.equals("3")) {
                    LoadFlexObjectV3 ma = new LoadFlexObjectV3(pigeonStoreEngine, flexobjectFiles);
                    ma.init();
                } else {
                    LoadFlexObject ma = new LoadFlexObject(pigeonStoreEngine, flexobjectFiles);
                    ma.init();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            runningThreadNum.countDown();
        }
    }

    static class LoadListThread extends Thread {
        private CountDownLatch runningThreadNum;

        public LoadListThread(CountDownLatch runningThreadNum) {
            this.runningThreadNum = runningThreadNum;
        }

        public void run() {
            try {
                if(StringUtils.isNotBlank(saasId)){
                    ((SaasPigeonEngine)(pigeonStoreEngine)).setCurrentMerchantId(saasId);
                }
                LoadList ma = new LoadList(pigeonStoreEngine, listFiles);
                ma.init();
            } catch (Exception e) {
                e.printStackTrace();
            }
            runningThreadNum.countDown();
        }
    }

    static class LoadIdServerThread extends Thread {
        private CountDownLatch runningThreadNum;

        public LoadIdServerThread(CountDownLatch runningThreadNum) {
            this.runningThreadNum = runningThreadNum;
        }

        public void run() {
            try {
                if(StringUtils.isNotBlank(saasId)){
                    ((SaasPigeonEngine)(pigeonStoreEngine)).setCurrentMerchantId(saasId);
                }
                LoadIdServer ma = new LoadIdServer(pigeonStoreEngine, idserverFiles);
                ma.init();
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

        LoadFlexObjectThread mfot = new LoadFlexObjectThread(runningThreadNum);
        mfot.start();

        LoadListThread mlt = new LoadListThread(runningThreadNum);
        mlt.start();

        LoadAtomThread mat = new LoadAtomThread(runningThreadNum);
        mat.start();

        LoadIdServerThread midgt = new LoadIdServerThread(runningThreadNum);
        midgt.start();

        runningThreadNum.await();

        long et = System.currentTimeMillis();

        System.out.println("PigeonLoad all ...... time ms = " + (et - st));
    }

    public static synchronized void doit(IPigeonStoreEngine pigeonStoreEngine, String DataDir) throws Exception {

        long st = System.currentTimeMillis();

        PigeonLoad.pigeonStoreEngine = pigeonStoreEngine;
        PigeonLoad.flexobjectVersion = "3";

        PigeonLoad.flexobjectFiles.clear();
        PigeonLoad.flexobjectFiles.add(DataDir + "/flexobject.txt");
        PigeonLoad.listFiles.clear();
        PigeonLoad.listFiles.add(DataDir + "/list.txt");
        PigeonLoad.atomFiles.clear();
        PigeonLoad.atomFiles.add(DataDir + "/atom.txt");
        PigeonLoad.idserverFiles.clear();
        PigeonLoad.idserverFiles.add(DataDir + "/idserver.txt");

        // init(config);

        final CountDownLatch runningThreadNum = new CountDownLatch(4);

        LoadFlexObjectThread mfot = new LoadFlexObjectThread(runningThreadNum);
        mfot.start();

        LoadListThread mlt = new LoadListThread(runningThreadNum);
        mlt.start();

        LoadAtomThread mat = new LoadAtomThread(runningThreadNum);
        mat.start();

        LoadIdServerThread midgt = new LoadIdServerThread(runningThreadNum);
        midgt.start();

        runningThreadNum.await();

        long et = System.currentTimeMillis();

        System.out.println("PigeonLoad all ...... time ms = " + (et - st));

    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("useage : java PigeonLoad load.conf");
            return;
        }

        String config = args[0];
        //String config = "pigeon-tools/resources/pigeonload.conf";

        doit(config);
    }

}
