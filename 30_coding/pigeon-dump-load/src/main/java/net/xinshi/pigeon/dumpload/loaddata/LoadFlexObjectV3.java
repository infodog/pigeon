package net.xinshi.pigeon.dumpload.loaddata;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.distributed.util.DefaultHashGenerator;
import net.xinshi.pigeon.dumpload.migration.PigeonMigration;
import net.xinshi.pigeon.flexobject.FlexObjectEntry;
import net.xinshi.pigeon.saas.SaasPigeonEngine;
import net.xinshi.pigeon.util.TimeTools;
import org.apache.commons.lang.StringUtils;

import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-5-11
 * Time: 下午5:58
 * To change this template use File | Settings | File Templates.
 */

public class LoadFlexObjectV3 {
    IPigeonStoreEngine pigeonStoreEngine;
    Vector<String> files;
    LoadDataFilesV3 ldfs;
    Logger logger = Logger.getLogger(LoadFlexObjectV3.class.getName());

    public LoadFlexObjectV3(IPigeonStoreEngine pigeonStoreEngine, Vector<String> files) {
        this.pigeonStoreEngine = pigeonStoreEngine;
        this.files = files;
    }

    public void init() throws Exception {
        long st = System.currentTimeMillis();
        ldfs = new LoadDataFilesV3("flexobjectV3", files, true);
        ldfs.init();
        int nw = 4;
        logger.info("loadFlexObjectV3 started.");

        CountDownLatch runningThreadNum = null;
        runningThreadNum = new CountDownLatch(nw);
        for (int i = 0; i < nw; i++) {
            Migration m = new Migration(runningThreadNum);
            m.start();
        }

        runningThreadNum.await();
        long et = System.currentTimeMillis();
        System.out.println("...... flexobject migration finished time ms = " + (et - st));
    }

    private class Migration extends Thread {

        private CountDownLatch runningThreadNum;

        public Migration(CountDownLatch runningThreadNum) {
            this.runningThreadNum = runningThreadNum;
        }

        public void run() {
            String threadid = "flexobjectV3 " + Thread.currentThread().getName() + " ";
            long st = System.currentTimeMillis();
            if(StringUtils.isNotBlank(PigeonLoad.saasId)){
                ((SaasPigeonEngine)(pigeonStoreEngine)).setCurrentMerchantId(PigeonLoad.saasId);
            }
            logger.info(threadid + " started.");
            int num = 0;
            boolean again = true;
            while (again) {
                String key = "";
                try {
                    HexRecord hr = ldfs.fetchHexRecord();
                    if (hr.getName() == null) {
                        continue;
                    }
                    key = hr.getName();
                    {
                        int hash = DefaultHashGenerator.hash(key);
                        if (!PigeonMigration.rightRange(hash)) {
                            System.out.println(threadid + " ?????? flexobject key bad hash range , key = " + key + ", hash = " + hash);
                            continue;
                        }
                    }
                    FlexObjectEntry entry = new FlexObjectEntry();
                    entry.setName(hr.getName());
                    entry.setBytesContent(hr.getBytes());
                    if (hr.getCompressed().equals("1")) {
                        entry.setCompressed(true);
                    } else {
                        entry.setCompressed(false);
                    }
                    if (hr.getString().equals("1")) {
                        entry.setString(true);
                    } else {
                        entry.setString(false);
                    }
                    while (true) {
                        try {
                            if (PigeonMigration.scan) {
                                ++num;
                                break;
                            }
                            pigeonStoreEngine.getFlexObjectFactory().saveFlexObject(entry);
                            if(hr.getFileName()!=null){
                                LoadFileUtils.saveLoadLog(hr.getFileName(),hr.getCurLine());
                            }
                            ++num;
                            if(num%1000==0){
                                logger.info(threadid +" - add flexobject:" + num);
                            }
                            break;
                        } catch (Exception e) {
                            if (e.getMessage().indexOf("too fast") >= 0) {
                                logger.info(e.getMessage());
                                Thread.sleep(1000);
                            } else {
                                System.out.println(threadid + " !!!!!! flexobject error ...... key : " + key);
                                Thread.sleep(1000);
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    if (key.length() > 0) {
                        System.out.println(threadid + "panic  !!!!!! flexobject panic ...... key : " + key + " error ");
                    }
                    break;
                }
            }
            long et = System.currentTimeMillis();
            System.out.println(TimeTools.getNowTimeString() + " " + threadid + "flexobject one thread finished time ms = " + (et - st) + " , count = " + num);
            runningThreadNum.countDown();
        }

    }
}
