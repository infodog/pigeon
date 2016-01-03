package net.xinshi.pigeon.dumpload.loaddata;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.distributed.util.DefaultHashGenerator;
import net.xinshi.pigeon.dumpload.migration.PigeonMigration;

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

public class LoadFlexObject {
    IPigeonStoreEngine pigeonStoreEngine;
    Vector<String> files;
    LoadDataFiles ldfs;

    public LoadFlexObject(IPigeonStoreEngine pigeonStoreEngine, Vector<String> files) {
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
        ldfs = new LoadDataFiles("flexobject", files, true);
        ldfs.init();
        int nw = 50;
        int count = 100;
        long one = (count + (nw - 1)) / nw;
        CountDownLatch runningThreadNum = null;
        if (one < 1) {
            runningThreadNum = new CountDownLatch(1);
            Migration m = new Migration(runningThreadNum);
            m.start();
        } else {
            runningThreadNum = new CountDownLatch(nw);
            for (int i = 0; i < nw; i++) {
                Migration m = new Migration(runningThreadNum);
                m.start();
            }
        }
        runningThreadNum.await();
        long et = System.currentTimeMillis();
        System.out.println("...... flexobject migration finished time ms = " + (et - st));
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
            String threadid = "flexobject " + Thread.currentThread().getName() + " ";
            long st = System.currentTimeMillis();
            System.out.println(threadid + "flexobject thread begin");
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
                                System.out.println(threadid + " ?????? flexobject key bad hash range , key = " + key + ", hash = " + hash);
                                continue;
                            }
                        }
                        while (true) {
                            try {
                                if (PigeonMigration.scan) {
                                    ++num;
                                    break;
                                }
                                pigeonStoreEngine.getFlexObjectFactory().addContent(elt.key, elt.val);
                                ++num;
                                break;
                            } catch (Exception e) {
                                if (e.getMessage().indexOf("DuplicateService.syncQueueOverflow()") >= 0) {
                                    System.out.println("migration flexobject : server is very busy ......");
                                    Thread.sleep(1000);
                                } else {
                                    e.printStackTrace();
                                    System.out.println(threadid + " !!!!!! flexobject error ...... key : " + key);
                                    Thread.sleep(1000);
                                }
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
            System.out.println(threadid + "flexobject one thread finished time ms = " + (et - st) + " , count = " + num);
            runningThreadNum.countDown();
        }

    }
}
