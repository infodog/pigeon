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

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-5-11
 * Time: 下午5:58
 * To change this template use File | Settings | File Templates.
 */

public class LoadIdServer {
    IPigeonStoreEngine pigeonStoreEngine;
    Vector<String> files;
    LoadDataFiles ldfs;

    public LoadIdServer(IPigeonStoreEngine pigeonStoreEngine, Vector<String> files) {
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
        ldfs = new LoadDataFiles("idserver", files, false);
        ldfs.init();
        int nw = 2;
        CountDownLatch runningThreadNum = null;
        runningThreadNum = new CountDownLatch(nw);
        for (int i = 0; i < nw; i++) {
            Migration m = new Migration(runningThreadNum);
            m.start();
        }

        runningThreadNum.await();
        long et = System.currentTimeMillis();
        System.out.println("...... idserver migration finished time ms = " + (et - st));
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
            String threadid = "idserver " + Thread.currentThread().getName() + " ";
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
                                System.out.println(threadid + " ?????? idserver key bad hash range , key = " + key + ", hash = " + hash);
                                continue;
                            }
                        }
                        while (true) {
                            try {
                                if (PigeonMigration.scan) {
                                    break;
                                }
                                long val = Long.valueOf(elt.val).longValue();
                                long id = 0;
                                while (id < val) {
                                    id = pigeonStoreEngine.getIdGenerator().getId(key);
                                    if (id > val) {
                                        break;
                                    }
                                    long delta = val - id;
                                    if (delta > 10000) {
                                        while (delta > 0) {
                                            id = pigeonStoreEngine.getIdGenerator().setSkipValue(key, delta + 1);
                                            delta = val - id;
                                        }
                                        break;
                                    }
                                }
                                ++num;
                                break;
                            } catch (Exception e) {
                                e.printStackTrace();

                                if (e.getMessage()!=null && e.getMessage().indexOf("DuplicateService.syncQueueOverflow()") >= 0) {
                                    System.out.println("migration idserver : server is very busy ......");
                                    Thread.sleep(1000);
                                } else {
                                    e.printStackTrace();
                                    System.out.println(threadid + " !!!!!! idserver error ...... key : " + key);
                                    Thread.sleep(1000);
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    if (key.length() > 0) {
                        System.out.println(threadid + "panic  !!!!!! idserver panic ...... key : " + key + " error ");
                    }
                    break;
                }
            }
            long et = System.currentTimeMillis();
            System.out.println(TimeTools.getNowTimeString() + " " + threadid + "idserver one thread finished time ms = " + (et - st) + " , count = " + num);
            runningThreadNum.countDown();
        }

    }
}
