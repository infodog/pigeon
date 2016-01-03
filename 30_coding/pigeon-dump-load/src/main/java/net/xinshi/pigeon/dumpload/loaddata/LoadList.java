package net.xinshi.pigeon.dumpload.loaddata;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.distributed.util.DefaultHashGenerator;
import net.xinshi.pigeon.dumpload.migration.PigeonMigration;
import net.xinshi.pigeon.list.ISortList;
import net.xinshi.pigeon.list.SortListObject;
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
 * Time: 下午5:14
 * To change this template use File | Settings | File Templates.
 */

public class LoadList {
    IPigeonStoreEngine pigeonStoreEngine;
    Vector<String> files;
    LoadDataFiles ldfs;
    Logger logger = Logger.getLogger(LoadList.class.getName());

    public LoadList(IPigeonStoreEngine pigeonStoreEngine, Vector<String> files) {
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
        ldfs = new LoadDataFiles("list", files, true);
        ldfs.init();
        int nw = 4;
        CountDownLatch runningThreadNum = new CountDownLatch(nw);
        for (int i = 0; i < nw; i++) {
            Migration m = new Migration(runningThreadNum);
            m.start();
        }


        runningThreadNum.await();
        long et = System.currentTimeMillis();
        logger.info("...... sortlist import finished time ms = " + (et - st));
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
            String threadid = "list " + Thread.currentThread().getName() + " ";
            long st = System.currentTimeMillis();
            if(StringUtils.isNotBlank(PigeonLoad.saasId)){
                ((SaasPigeonEngine)(pigeonStoreEngine)).setCurrentMerchantId(PigeonLoad.saasId);
            }
            int nn = 0;
            int num = 0;
            int abandon = 0;
            long t1 = System.currentTimeMillis();
            boolean again = true;
            while (again) {
                String key = "";
                try {
                    HexRecord hr = ldfs.fetchHexRecord();
                    key_val kv = new key_val(hr.getName(), hr.getValue());
                    List<key_val> records = new ArrayList();
                    records.add(kv);

                    for (key_val elt : records) {
                        if (elt == null || elt.key == null) {
                            again = false;
                            break;
                        }
                        key = elt.key;
                        {
                            int hash = DefaultHashGenerator.hash(key);
                            if (!PigeonMigration.rightRange(hash)) {
                                System.out.println(threadid + " ?????? sortlist key bad hash range , key = " + key + ", hash = " + hash);
                                continue;
                            }
                        }
                        String[] strObjs = elt.val.split(";");
                        for (String strObj : strObjs) {
                            String[] fields = strObj.split(",");
                            if (fields.length == 2) {
                                SortListObject obj = new SortListObject();
                                String oid = fields[0];
                                if (oid == null || "".equals(oid.trim())) {
                                    ++abandon;
                                    logger.warning("key=" + fields[1] + " was abandon,total abandon=" + abandon);
                                    continue;
                                }
                                String okey = fields[1];
                                if (okey == null || "".equals(okey.trim())) {
                                    ++abandon;
                                    continue;
                                }
                                obj.setObjid(fields[0]);
                                obj.setKey(fields[1]);
                                while (true) {
                                    try {
                                        if (PigeonMigration.scan) {
                                            break;
                                        }
                                        ISortList list = pigeonStoreEngine.getListFactory().getList(key, true);
                                        boolean rb = list.add(obj);
                                        if (!rb) {
                                            // throw new Exception("list Add SortObj failed ... " + key);
                                        }
                                        break;
                                    } catch (Exception e) {
                                        if (e.getMessage().indexOf("too fast") >= 0) {
                                            System.out.println("migration list : server is very busy , too fast......");
                                            Thread.sleep(1000);
                                        } else {
                                            // throw e;
                                            //e.printStackTrace();
                                            logger.warning(key + " <-- key : " + e.getMessage());
                                            if (e.getMessage().indexOf("list obj key has invalid chars") >= 0) {
                                                break;
                                            }
                                            if (e.getMessage().indexOf("list obj id has invalid chars") >= 0) {
                                                break;
                                            }
                                            //Thread.sleep(1000);
                                        }
                                    }
                                }
                                ++nn;
                                ++num;
                                if (nn > 1000) {
                                    // System.out.println(threadid + " loop list finished " + nn);
                                    long t2 = System.currentTimeMillis();
                                    logger.info(threadid + " addlist " + nn + " ,total=" + num + " , cost ms = " + (t2 - t1));
                                    nn = 0;
                                    t1 = t2;
                                }
                            } else if (!strObj.equals("\n")) {
                                System.out.println("list abandon obj : " + strObj);
                                ++abandon;
                            }
                        }
                    }
                    if(hr.getFileName()!=null){
                        LoadFileUtils.saveLoadLog(hr.getFileName(),hr.getCurLine());
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    if (key.length() > 0) {
                        logger.info(threadid + "panic  !!!!!! sortlist panic ...... key : " + key + " error ");
                    }
                    break;
                }
            }
            long et = System.currentTimeMillis();
            System.out.println(TimeTools.getNowTimeString() + " " + threadid + "report sortlist one thread finished time ms = " + (et - st) + " , count = " + num + ", abandon = " + abandon);
            runningThreadNum.countDown();
        }
    }
}
