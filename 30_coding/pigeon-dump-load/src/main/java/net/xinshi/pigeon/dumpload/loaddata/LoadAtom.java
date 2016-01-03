package net.xinshi.pigeon.dumpload.loaddata;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.distributed.util.DefaultHashGenerator;
import net.xinshi.pigeon.dumpload.migration.PigeonMigration;
import net.xinshi.pigeon.saas.SaasPigeonEngine;
import net.xinshi.pigeon.util.TimeTools;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
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

public class LoadAtom {
    IPigeonStoreEngine pigeonStoreEngine;
    Vector<String> files;
    LoadDataFiles ldfs;

    Logger logger = Logger.getLogger(LoadAtom.class.getName());
    public LoadAtom(IPigeonStoreEngine pigeonStoreEngine, Vector<String> files) {
        this.pigeonStoreEngine = pigeonStoreEngine;
        this.files = files;
    }

    class key_val {
        public String key;
        public String val;

        key_val(String key, String val) {
            this.key = key;
            this.val = val;
        }
    }

    public void init() throws Exception {
        long st = System.currentTimeMillis();
        logger.info("pigeonStoreEngine==null?" + (pigeonStoreEngine==null));
        ldfs = new LoadDataFiles("atom", files, false);
        ldfs.init();
        int nw = 4;

        CountDownLatch runningThreadNum = null;

        runningThreadNum = new CountDownLatch(nw);
        for (int i = 0; i < nw; i++) {
            Migration m = new Migration(runningThreadNum);
            m.start();
        }
        runningThreadNum.await();
        long et = System.currentTimeMillis();
        System.out.println("...... atom migration finished time ms = " + (et - st));
    }

    List<key_val> getKVs() throws Exception {
        List<key_val> listKV = new ArrayList<key_val>();
        HexRecord hr = ldfs.fetchHexRecord();
        key_val kv = new key_val(hr.getName(), hr.getValue());
        listKV.add(kv);
        return listKV;
    }

    private class Migration extends Thread {

        private CountDownLatch runningThreadNum;

        public Migration(CountDownLatch runningThreadNum) {
            this.runningThreadNum = runningThreadNum;
        }

        public void run() {
            String threadid = "atom " + Thread.currentThread().getName() + " ";
            long st = System.currentTimeMillis();
            if(StringUtils.isNotBlank(PigeonLoad.saasId)){
                ((SaasPigeonEngine)(pigeonStoreEngine)).setCurrentMerchantId(PigeonLoad.saasId);
            }
            int num = 0;
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
                                continue;
                            }
                        }
                        long val = Long.valueOf(elt.val).longValue();
                        while (true) {
                            try {
                                if (PigeonMigration.scan) {
                                    ++num;
                                    break;
                                }
                                boolean rb = pigeonStoreEngine.getAtom().createAndSet(key, (int) val);
                                if (!rb) {
                                    throw new Exception("panic !!! atom failed : " + key);
                                }
                                ++num;
                                if(num%10000 ==0 ){
                                    logger.info(threadid + "-" + "loadatom,number=" + num);
                                }
                                break;
                            } catch (Exception e) {
                                e.printStackTrace();
                                if (e.getMessage()!=null && e.getMessage().indexOf("DuplicateService.syncQueueOverflow()") >= 0) {
                                    Thread.sleep(1000);
                                } else {
                                    e.printStackTrace();
                                    Thread.sleep(1000);
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    if (key.length() > 0) {
                        System.out.println(threadid + "panic  !!!!!! atom panic ...... key : " + key + " error ");
                    }
                    break;
                }
            }
            long et = System.currentTimeMillis();
            System.out.println(TimeTools.getNowTimeString() + " " + threadid + "atom one thread finished time ms = " + (et - st) + " , count = " + num);
            runningThreadNum.countDown();
        }

    }
}
