package net.xinshi.pigeon.pigeonclient;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.adapter.StaticPigeonEngine;
import net.xinshi.pigeon.adapter.impl.DistributedPigeonEngine;
import net.xinshi.pigeon.adapter.impl.NormalPigeonEngine;
import net.xinshi.pigeon.atom.IIntegerAtom;
import net.xinshi.pigeon.filesystem.IFileSystem;
import net.xinshi.pigeon.filesystem.impl.PigeonFileSystem;
import net.xinshi.pigeon.flexobject.FlexObjectEntry;
import net.xinshi.pigeon.idgenerator.IIDGenerator;
import net.xinshi.pigeon.list.IListFactory;
import net.xinshi.pigeon.list.ISortList;
import net.xinshi.pigeon.list.SortListObject;
import net.xinshi.pigeon.saas.backup.ClientBackup;
import net.xinshi.pigeon.server.standalongserver.PigeonListener;
import net.xinshi.pigeon.util.CommonTools;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.testng.Assert;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-1-29
 * Time: ����10:57
 * To change this template use File | Settings | File Templates.
 */
public class ClientTest {

    static NormalPigeonEngine pigeonEngine;
    static IPigeonStoreEngine pigeonStoreEngine;
    static IFileSystem fileSystem;
    static Logger logger = Logger.getLogger("pigeonServerTest");

    static long lockIndex = 0;


    static public class SubThread extends Thread {
        private CountDownLatch runningThreadNum;
        private int i = -1;

        public SubThread(CountDownLatch runningThreadNum, int n) {
            this.runningThreadNum = runningThreadNum;
            i = n;
        }

        @Override
        public void run() {
            try {
                String s = StringUtils.repeat("adcvc", 1024);
                String name = Thread.currentThread().getName() + "largeObj" + System.currentTimeMillis();
                System.out.println("thread name = " + name);
                long begin = System.currentTimeMillis();
                /*
                String str = "tls..." + i;
                for (int i = 0; i < 100; i++) {
                    String rs = pigeonStoreEngine.getFlexObjectFactory().getContent(str);
                }
                if (i % 4 == 0) {
                    pigeonStoreEngine.getFlexObjectFactory().setTlsMode(true);
                }
                for (int i = 0; i < 100; i++) {
                    String rs = pigeonStoreEngine.getFlexObjectFactory().getContent(str);
                }
                if (i == 4) {
                    pigeonStoreEngine.getFlexObjectFactory().setTlsMode(false);
                }
                for (int i = 0; i < 100; i++) {
                    String rs = pigeonStoreEngine.getFlexObjectFactory().getContent(str);
                }
                if (i == 8) {
                    pigeonStoreEngine.getFlexObjectFactory().setTlsMode(false);
                }
                for (int i = 0; i < 100; i++) {
                    String rs = pigeonStoreEngine.getFlexObjectFactory().getContent(str);
                }
                */
                for (int i = 0; i < 100000; i++) {
                    try {
                        pigeonStoreEngine.getFlexObjectFactory().addContent(name + "_" + i, s);
                    } catch (Exception e) {
                        // e.printStackTrace();
                        if (e.getMessage().indexOf("syncQueueOverflow(") >= 0) {
                            --i;
                        }
                    }
                }
                long end = System.currentTimeMillis();
                long d = end - begin;
                logger.severe("   ------       " + this.i + " ---------------------------------flexObjectSaveLargeStringTest : " + d + "ms");

                // test_list_state_2();

                // testGetGetIds3();
                // testAtom000();

                // testlockindex();


            } catch (Exception e) {
                e.printStackTrace();
            }
            runningThreadNum.countDown();
        }
    }


    public static void main(String[] args) throws Exception {
        logger.log(Level.INFO, "setup");
        createPigeonClient();
        createPigeonFileSystem();
        pigeonEngine = new NormalPigeonEngine();
        pigeonEngine.setPigeonStoreEngine(pigeonStoreEngine);
        pigeonEngine.setFileSystem(fileSystem);
        StaticPigeonEngine.pigeon = pigeonEngine;

        {
            pigeonStoreEngine.getFlexObjectFactory().saveContent("appr_o_common_672331_p_57672", "{\"buyerInfo\":{\"loginId\":\"DL7578QQ\",\"logo\":\"\",\"userId\":\"u_160353\",\"lastModifyUserId\":\"\",\"realName\":\"\",\"lastModifyTime\":\"\"},\"orderPayTime\":\"\",\"sellerInfo\":{\"name_cn\":\"默认商家\",\"name_en\":\"DefaultMerchant\",\"merchantVersionId\":\"50000\",\"merchantId\":\"m_100\"},\"productInfo\":{\"logo\":\"http://60.166.15.148/upload/s1/2011/12/20/213755_60X60.jpg\",\"productVersionId\":\"304983\",\"skuId\":\"sku_57932\",\"unitPrice\":\"\",\"productName\":\"七度空间迷你巾（纯棉18片）\",\"productId\":\"p_57672\"},\"hourOfDay\":10,\"id\":\"appr_o_common_672331_p_57672\",\"orderCreateTime\":\"1375323437819\",\"process\":true,\"keyTime\":1376876225534,\"msgId\":\"msg_571705\",\"U2P\":{\"isAnonymity\":false,\"createTime\":\"1376876225533\",\"effect\":0,\"star\":\"1\",\"modify\":0,\"apprType\":\"U2P\",\"comment\":\"都是往年的货，上面的灰都是厚厚的一层，很不满意，只有生产日期，下次再也不会买了。\",\"isCanModify\":true},\"isPayOnline\":false,\"orderId\":\"o_common_672331\"}");
            {
                testGetGetIds();

               //  pigeonStoreEngine.getAtom().createAndSet("test1", 100);

                Long r = pigeonStoreEngine.getAtom().get("test1");

                boolean rb0 = pigeonStoreEngine.getAtom().greaterAndInc("test1", 10, 1);

                boolean rb = pigeonStoreEngine.getAtom().lessAndInc("test1", 180, -1);

                ISortList sl = pigeonStoreEngine.getListFactory().getList("1234567", true);

                for (int i = 0; i < 5000; i++) {
                    boolean rbx = sl.delete(new SortListObject("1234r5" + i, "6789ran" + i));
                }

                for (int i = 0; i < 100; i++) {
                    String str = "tls..." + i;
                    pigeonStoreEngine.getFlexObjectFactory().addContent(str, str);
                }


                String s = pigeonStoreEngine.getFlexObjectFactory().getContent("exp_orderAll_80000");
                pigeonStoreEngine.getFlexObjectFactory().saveContent("exp_orderAll_80000", "aabb123你好吗$$$？xxx中國軟體好啦。。。！！");
                s = pigeonStoreEngine.getFlexObjectFactory().getContent("exp_orderAll_80000");
                pigeonStoreEngine.getFlexObjectFactory().addContent("exp_orderAll_80000", "");
                s = pigeonStoreEngine.getFlexObjectFactory().getContent("exp_orderAll_80000");
                int i = 0;
            }

            long startTime = System.currentTimeMillis();
            int max = 20;
            CountDownLatch runningThreadNum = new CountDownLatch(max);
            for (int i = 0; i < max; i++) {
                new SubThread(runningThreadNum, i).start();
            }
            System.out.println("create threads over!");
            runningThreadNum.await();
            long endTime = System.currentTimeMillis();
            System.out.println("SubThread total time : " + (endTime - startTime) + " ms");

            Thread.sleep(1000 * 10000);

            saveFlexObjectsTest();

            {
                String s = pigeonStoreEngine.getFlexObjectFactory().getContent("exp_orderAll_80000");
                pigeonStoreEngine.getFlexObjectFactory().saveContent("exp_orderAll_80000", "aabb123你好吗$$$？xxx中國軟體好啦。。。！！");
                s = pigeonStoreEngine.getFlexObjectFactory().getContent("exp_orderAll_80000");
                int i = 0;
            }

            getSortListObjectTest();

            testAtom000();

            testGetGetIds();
        }

        {
            String s = pigeonStoreEngine.getFlexObjectFactory().getContent("exp_orderAll_80000");
            int i = 0;
        }

        {
            ISortList isl = pigeonStoreEngine.getListFactory().getList("test12332100", true);
            SortListObject slo;
            for (int i = 0; i < 10000; i += 2) {
                slo = new SortListObject(String.format("%09d", i), String.format("%09d", i));
                isl.add(slo);
            }
            for (int i = 1; i < 10000; i += 2) {
                slo = new SortListObject(String.format("%09d", i), String.format("%09d", i));
                isl.add(slo);
            }
            // List<SortListObject> list= isl.getRange(0, -1);
            slo = new SortListObject(String.format("%09d", 4), String.format("%09d", 4));
            long pos = isl.getLessOrEqualPos(slo);
            int x = 0;
        }

        {
            String dir = ClientBackup.backup(null, "c:/xxxyyy");
            if (dir != null) {
                System.out.println(dir);
                //return;
            }
        }
        {
            String dir = ClientBackup.backup("saasAccount_50000", "c:/xxxyyy");
            if (dir != null) {
                System.out.println(dir);
                return;
            }
        }

        {
            // pigeonStoreEngine.getAtom().createAndSet("test1", 100);

            Long r = pigeonStoreEngine.getAtom().get("test1");

            boolean rb0 = pigeonStoreEngine.getAtom().greaterAndInc("test1", 180, -1);

            boolean rb = pigeonStoreEngine.getAtom().lessAndInc("test1", 180, -1);

            ISortList sl = pigeonStoreEngine.getListFactory().getList("1234567", true);

            boolean rbx = sl.add(new SortListObject("1234r5", "6789ran"));

            for (int i = 0; i < 100; i++) {
                String str = "tls..." + i;
                pigeonStoreEngine.getFlexObjectFactory().addContent(str, str);
            }

            /* {
                File f = new File("c:\\wptpass.infozip");
                f = f.getAbsoluteFile();
                //fileSystem.addFile(f,"pigeonclient.conf");
                long currentTime = System.currentTimeMillis();
                String s = "" + currentTime + "_testAddFile";
                String fileId = fileSystem.addFile(f, "170000.JPG");
                if (fileId == null) {
                    throw new Exception("addFile return null." + f.getAbsolutePath());
                }
            }*/


            /*  test_list_state();

         try {
             pigeonStoreEngine.getFlexObjectFactory().saveContent("double", "hello world");
         } catch (Exception e) {
             e.printStackTrace();
         }

         try {
             FlexObjectEntry e = pigeonStoreEngine.getFlexObjectFactory().getFlexObject("m_50000:obj:app_infoscape.usermgt");
         } catch (Exception e) {
             e.printStackTrace();
         }

         try {
             String s0 = pigeonStoreEngine.getFlexObjectFactory().getContent("m_50000:obj:app_infoscape.usermgt");
         } catch (Exception e) {
             e.printStackTrace();
         }

         List<String> lst = new Vector<String>(2);
         lst.add("m_50000:obj:app_infoscape.usermgt");
         lst.add("m_50000:obj:app_infoscape.usermgt");

         List<FlexObjectEntry> re = pigeonStoreEngine.getFlexObjectFactory().getFlexObjects(lst);

         List<String> res = pigeonStoreEngine.getFlexObjectFactory().getContents(lst);

         for (String s : res) {
             System.out.println(s);
         }

         int i = 0;*/

            /* for (int i = 0; i < 100; i++) {
             pigeonStoreEngine.getLock().Lock("hello");
             System.out.println("lock ok");
             pigeonStoreEngine.getLock().Lock("hello");
             System.out.println("lock ok");
             Thread.sleep(1000);
             pigeonStoreEngine.getLock().Unlock("hello");
             System.out.println("unlock ok");
             pigeonStoreEngine.getLock().Unlock("hello");
             System.out.println("unlock ok");
         }

         Thread.sleep(1000 * 50000);

         pigeonStoreEngine.getFlexObjectFactory().addContent("你好呀%#", "过啊*&#￥");
         String h = pigeonStoreEngine.getFlexObjectFactory().getContent("你好呀%#");*/

            /* int i = 0;
                        System.out.println("start ...");
                        long ls = System.currentTimeMillis();
                        for (int j = 0; j < 10000; j++) {
                            pigeonStoreEngine.getLock().Lock("hello");
                            // System.out.println("lock ok");
                            pigeonStoreEngine.getLock().Unlock("hello");
                            // System.out.println("unlock ok");
                        }
                        long le = System.currentTimeMillis();

                        System.out.println("lock 10000 time = " + (le - ls) + " (ms)");
            */
            /*  pigeonStoreEngine.getLock().Lock("hello");
          System.out.println("lock ok");
          Thread.sleep(1000 * 30);
          pigeonStoreEngine.getLock().Unlock("hello");
          System.out.println("unlock ok");
          Thread.sleep(1000 * 3000);*/
        }
/*
        {
            File f = new File("c:/8.jpg");
            f = f.getAbsoluteFile();
            String fileId = fileSystem.addFile(f, "8.jpg");
            if (fileId == null) {
                throw new Exception("addFile return null." + f.getAbsolutePath());
            }
            fileSystem.genRelatedFile(fileId, "200X200");
            String url = fileSystem.getUrl(fileId);
            int i = 0;
        }

        String t0 = pigeonStoreEngine.getFlexObjectFactory().getContent("001");
        String s = StringUtils.repeat("adcvc", 1024);
        pigeonStoreEngine.getFlexObjectFactory().saveContent("001", s);*/

        while (true) {
            try {

                /*testAtomCreateAndSet000();   */

                {
                    long startTime = System.currentTimeMillis();
                    int max = 10;
                    CountDownLatch runningThreadNum = new CountDownLatch(max);
                    for (int i = 0; i < max; i++) {
                        new SubThread(runningThreadNum, i).start();
                    }
                    System.out.println("create threads over!");
                    long startTime2 = System.currentTimeMillis();


                    {
                        List<String> names = new ArrayList<String>();
                        for (int i = -10; i < 130; i++) {
                            String str = "tls..." + i;
                            names.add(str);
                        }
                        List<String> rl = pigeonStoreEngine.getFlexObjectFactory().getContents(names);
                        if (rl.size() != names.size()) {
                            System.out.print("bad getContents");
                        }
                    }
                    long endTime = System.currentTimeMillis();
                    System.out.println("1  main total time : " + (endTime - startTime2) + " ms");

                    pigeonStoreEngine.getFlexObjectFactory().setTlsMode(true);

                    startTime2 = System.currentTimeMillis();


                    {
                        List<String> names = new ArrayList<String>();
                        for (int i = -10; i < 130; i++) {
                            String str = "tls..." + i;
                            names.add(str);
                        }
                        List<String> rl = pigeonStoreEngine.getFlexObjectFactory().getContents(names);
                        if (rl.size() != names.size()) {
                            System.out.print("bad getContents");
                        }
                    }
                    endTime = System.currentTimeMillis();
                    System.out.println("2  main total time : " + (endTime - startTime2) + " ms");

                    {
                        List<String> names = new ArrayList<String>();
                        for (int i = -10; i < 130; i++) {
                            String str = "tls..." + i;
                            names.add(str);
                        }
                        List<String> rl = pigeonStoreEngine.getFlexObjectFactory().getContents(names);
                        if (rl.size() != names.size()) {
                            System.out.print("bad getContents");
                        }
                    }
                    endTime = System.currentTimeMillis();
                    System.out.println("3  main total time : " + (endTime - startTime2) + " ms");

                    pigeonStoreEngine.getFlexObjectFactory().setTlsMode(false);

                    {
                        List<String> names = new ArrayList<String>();
                        for (int i = -10; i < 130; i++) {
                            String str = "tls..." + i;
                            names.add(str);
                        }
                        List<String> rl = pigeonStoreEngine.getFlexObjectFactory().getContents(names);
                        if (rl.size() != names.size()) {
                            System.out.print("bad getContents");
                        }
                    }
                    endTime = System.currentTimeMillis();
                    System.out.println("4  main total time : " + (endTime - startTime2) + " ms");


                    runningThreadNum.await();
                    endTime = System.currentTimeMillis();
                    System.out.println("main total time : " + (endTime - startTime) + " ms");
                    System.out.println("main total time : " + (endTime - startTime2) + " ms");
                    break;
                }


                /*  long ls = System.currentTimeMillis();
                  test_list_state();

                *//*  testAtomCreateAndSet();
                testGreaterAndInc();
                testGetAtoms();
                test_atom_state();*//*

                // testGetGetIds();

                long le = System.currentTimeMillis();

                System.out.print("time ms : " + (le - ls));*/

                /*     long t = 0;
                                for (int i = 0; i < 10; i++) {
                                    t += flexObjectAddLargeStringTest();
                                }
                                System.out.println("total = " + t + ", avg = " + (t / 10));
                */
                ///testAtomCreateAndSet();
                ///flexObjectSaveLargeStringTest2();
                // test_list_state();

                /* new Thread(new Runnable() {
                    public void run() {
                        while (true) {
                            try {
                                testAtomCreateAndSet();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }).start();

                new Thread(new Runnable() {
                    public void run() {
                        while (true) {
                            try {
                                flexObjectSaveLargeStringTest2();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }).start();

                new Thread(new Runnable() {
                    public void run() {
                        while (true) {
                            try {
                                test_list_state();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }).start();

                new Thread(new Runnable() {
                    public void run() {
                        while (true) {
                            try {
                                testGetGetIdsState();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }).start();*/

                /// break;
                // test_atom_state();

                // testGetGetIdsState();
/*

                flexObjectSaveTest();
                flexObjectGetContentsTest();
                flexObjectSaveLargeStringTest();
                flexObjectAddLargeStringTest();
                saveFlexObjectsTest();
                testDeleteObject();

                //////////////////////////

                testAddListObject();
                testGetRange();
                testDelete();
                testIsExists();
                getSortListObjectTest();

                /////////////////////////

                testAtomCreateAndSet();
                testGreaterAndInc();
                testGetAtoms();

                /////////////////////////

                testGetGetIds();

                ////////////////////////
                *//*//**//* *//*//*

                testAddFile();
                testGenFile();
                testDeleteFile();*/

                // logger.severe(" -- sleep 30 s -- ");
                // break;
                // Thread.sleep(1000 * 30);


            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void createPigeonClient() throws Exception {
        /// File f = new File("pigeon-server/src/test/resources/pigeonclient2.conf");
        /// pigeonStoreEngine = new SingleServerEngine(f.getAbsolutePath());
        //File f = new File("pigeon-server/src/test/resources/pigeonclient2.conf");
        //pigeonStoreEngine = new NettySingleServerEngine(f.getAbsolutePath());
        File f = new File("pigeon-server/src/test/resources/pigeonclient2.conf");
        pigeonStoreEngine = new DistributedPigeonEngine(f.getAbsolutePath());
    }

    public static void createPigeonFileSystem() throws Exception {
        fileSystem = new PigeonFileSystem(pigeonStoreEngine, // "pigeon-server/src/test/resources/pigeonclient2.conf", true
                "pigeon-server/src/test/resources/pigeonfilesystem.conf");
    }

    public static void testlockindex() throws Exception {
        for (int i = 0; i < 1000; i++) {
            pigeonStoreEngine.getLock().Lock("hello");
            long x = ++lockIndex;
            //Thread.sleep(10);
            if (x != lockIndex) {
                System.out.println("error................... x != lockIndex");
            }
            pigeonStoreEngine.getLock().Unlock("hello");
            System.out.println(Thread.currentThread().getName() + " now index ...... = " + x);
        }
    }

    ////Test
    public static void flexObjectSaveTest() throws Exception {
        try {
            String content = "222222";
            pigeonStoreEngine.getFlexObjectFactory().saveContent("testObject", content);
            String s = pigeonStoreEngine.getFlexObjectFactory().getContent("testObject");
            if (!StringUtils.equals(s, content)) {
                throw new Exception(s);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    ////Test
    public static void flexObjectGetContentsTest() throws Exception {
        try {

            String[] names = new String[20];
            String[] contents = new String[20];
            for (int i = 0; i < names.length; i++) {
                names[i] = "testObj" + i;
                contents[i] = "content" + i;
            }


            for (int i = 0; i < names.length; i++) {
                pigeonStoreEngine.getFlexObjectFactory().saveContent(names[i], contents[i]);
            }

            Vector vNames = new Vector();
            for (int i = 0; i < names.length; i++) {
                vNames.add(names[i]);
            }
            List<String> vContents = pigeonStoreEngine.getFlexObjectFactory().getContents(vNames);

            for (int i = 0; i < names.length; i++) {
                if (!StringUtils.equals(contents[i], vContents.get(i))) {
                    throw new Exception("getContents error.");
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

    }

    public static void flexObjectSaveLargeStringTest2() throws Exception {
        String s = StringUtils.repeat("adcvc", 1024);
        String name = "largeObj" + System.currentTimeMillis();
        long begin = System.currentTimeMillis();
        logger.log(Level.WARNING, "flexObjectSaveLargeStringTest");
        for (int i = 0; i < 10000; i++) {
            try {
                pigeonStoreEngine.getFlexObjectFactory().saveContent(name + "_" + i, s);
                // Thread.sleep(1000 * 10);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        long end = System.currentTimeMillis();
        logger.severe("flexObjectSaveLargeStringTest : " + (end - begin) + "ms");
        // System.exit(0);
        List<String> names = new Vector<String>();
        for (int i = 0; i < 10000; i++) {
            if (names.size() == 10) {
                // logger.log(Level.WARNING, "getContents:" + names.toString());
                List<String> contents = null;
                try {
                    contents = pigeonStoreEngine.getFlexObjectFactory().getContents(names);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                // logger.log(Level.WARNING, "getContents finished:" + names.toString());
                for (String content : contents) {
                    if (!StringUtils.equals(content, s)) {
                        logger.log(Level.WARNING, "not equal," + content);
                    }
                }
                names.clear();
            }
            names.add(name + "_" + i);

        }
    }

    ////Test
    public static void flexObjectSaveLargeStringTest() throws Exception {
        String s = StringUtils.repeat("adcvc", 1024);
        String name = "largeObj" + System.currentTimeMillis();
        long begin = System.currentTimeMillis();
        logger.log(Level.WARNING, "flexObjectSaveLargeStringTest");
        for (int i = 0; i < 10000; i++) {
            pigeonStoreEngine.getFlexObjectFactory().saveContent(name + "_" + i, s);
        }
        long end = System.currentTimeMillis();


        logger.severe("flexObjectSaveLargeStringTest : " + (end - begin) + "ms");
        List<String> names = new Vector<String>();
        for (int i = 0; i < 10000; i++) {
            if (names.size() == 10) {
                logger.log(Level.WARNING, "getContents:" + names.toString());

                List<String> contents = pigeonStoreEngine.getFlexObjectFactory().getContents(names);
                logger.log(Level.WARNING, "getContents finished:" + names.toString());
                for (String content : contents) {
                    if (!StringUtils.equals(content, s)) {
                        logger.log(Level.WARNING, "not equal," + content);
                    }
                }
                names.clear();
            }
            names.add(name + "_" + i);

        }
    }

    //// Test
    public static long flexObjectAddLargeStringTest() throws Exception {
        String s = StringUtils.repeat("adcvc", 1024);
        String name = "largeObj" + System.currentTimeMillis();
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            pigeonStoreEngine.getFlexObjectFactory().addContent(name + "_" + i, s);
        }
        long end = System.currentTimeMillis();


        long d = end - begin;
        logger.severe("---------------------------------flexObjectSaveLargeStringTest : " + d + "ms");
        return d;
    }

    ////Test
    public static void saveFlexObjectsTest() throws Exception {
        String s = StringUtils.repeat("adcvc", 1024);
        String name = "largeObj" + System.currentTimeMillis();
        long begin = System.currentTimeMillis();
        List<FlexObjectEntry> objs = new Vector<FlexObjectEntry>();

        for (int i = 0; i < 200000; i++) {
            FlexObjectEntry obj = new FlexObjectEntry();
            obj.setName(name + "_" + i);
            obj.setAdd(true);
            obj.setCompressed(true);
            obj.setString(true);
            obj.setBytesContent(CommonTools.zip(s.getBytes("UTF-8")));
            // objs.add(obj);
            pigeonStoreEngine.getFlexObjectFactory().saveFlexObject(obj);
        }
        // pigeonStoreEngine.getFlexObjectFactory().saveFlexObjects(objs);
        long end = System.currentTimeMillis();

        logger.log(Level.WARNING, "flexObjectSaveLargeStringTest : " + (end - begin) + "ms");
    }

    ////Test
    public static void testDeleteObject() throws Exception {
        String[] names = new String[20];
        String[] contents = new String[20];
        for (int i = 0; i < names.length; i++) {
            names[i] = "testObj" + i;
            contents[i] = "";
        }


        for (int i = 0; i < names.length; i++) {
            pigeonStoreEngine.getFlexObjectFactory().saveContent(names[i], contents[i]);
        }

    }

    ////Test
    public static void testAddListObject() throws Exception {
        IListFactory listFactory = pigeonStoreEngine.getListFactory();

        String listId = "test" + System.currentTimeMillis();
        ISortList list = listFactory.getList(listId, true);
        SortListObject srObj = new SortListObject("test1", "obj1");
        list.add(srObj);


        boolean result = list.add(srObj);
        if (result != false) {
            throw new Exception("error. duplicated obj.");
        }

        //Assert.assertEquals(list.getSize(), 1);

        srObj.setKey("test2");
        list.add(srObj);

        //Assert.assertEquals(list.getSize(), 2);


    }

    static void test_list_state() {
        IListFactory listFactory = pigeonStoreEngine.getListFactory();
        long ll = System.currentTimeMillis();
        String threadid = Thread.currentThread().getName();
        for (int i = 0; i < 10000; i++) {
            String listId = threadid + "test" + ll + i;
            try {
                ISortList list = listFactory.getList(listId, true);
                SortListObject srObj = new SortListObject("test" + i, "obj" + i);
                list.add(srObj);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        for (int i = 0; i < 5678; i++) {
            String listId = threadid + "test" + ll + i;
            try {
                ISortList list = listFactory.getList(listId, true);
                List<SortListObject> objs = list.getRange(0, -1);
                if (objs != null) {
                    for (SortListObject slo : objs) {
                        list.delete(slo);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        long le = System.currentTimeMillis();
        System.out.println(threadid + " cost ms : " + (le - ll));
    }

    static void test_list_state_2() {
        IListFactory listFactory = pigeonStoreEngine.getListFactory();
        long ll = System.currentTimeMillis();
        String threadid = Thread.currentThread().getName();
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 1000; i++) {
                String listId = threadid + "test" + ll + j;
                try {
                    ISortList list = listFactory.getList(listId, true);
                    SortListObject srObj = new SortListObject("test" + i, "obj" + i);
                    list.add(srObj);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 678; i++) {
                String listId = threadid + "test" + ll + j;
                try {
                    ISortList list = listFactory.getList(listId, true);
                    List<SortListObject> objs = list.getRange(0, -1);
                    if (objs != null) {
                        for (SortListObject slo : objs) {
                            list.delete(slo);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        long le = System.currentTimeMillis();
        System.out.println(threadid + " cost ms : " + (le - ll));
    }

    ////Test
    public static void testGetRange() throws Exception {
        IListFactory listFactory = pigeonStoreEngine.getListFactory();

        String listId = "testx" + System.currentTimeMillis();
        ISortList list = listFactory.getList(listId, true);
        SortListObject srObj = new SortListObject();
        srObj.setObjid("obj1");
        for (int i = 0; i < 1200; i++) {
            String key = "test" + CommonTools.getComparableString(i + 2, 5);
            srObj.setKey(key);
            list.add(srObj);
        }


        List<SortListObject> objs = list.getRange(0, -1);
        System.out.println("--------------list id:" + listId);
        //Assert.assertEquals(1200, objs.size());
        if (1200 != objs.size()) {
            //Thread.sleep(100000000);
        }
        for (int i = 0; i < objs.size(); i++) {
            SortListObject sobj1 = objs.get(i);
            String key = "test" + CommonTools.getComparableString(i + 2, 5);
            //Assert.assertEquals(sobj1.getKey(), key);
            //Assert.assertEquals(sobj1.getObjid(), "obj1");
        }

    }

    ////Test
    public static void testDelete() throws Exception {
        IListFactory listFactory = pigeonStoreEngine.getListFactory();

        long begin = System.currentTimeMillis();

        String listId = "test1327914804343"; //"test" + System.currentTimeMillis();
        ISortList list = listFactory.getList(listId, true);

        long n = list.getSize();
        System.out.println("list size : " + list.getSize());

        SortListObject srObj = new SortListObject();
        srObj.setObjid("obj1");
        Random rand = new Random();
        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < 2000; i++) {
                String key = "test" + rand.nextInt();
                srObj.setKey(key);
                list.add(srObj);
            }


            System.out.println("list size : " + list.getSize());
            List<SortListObject> objs = list.getRange(0, -1);
            Collections.shuffle(objs);
            //Assert.assertEquals(8000, objs.size());
            for (int i = 0; i < objs.size() / 2; i++) {
                SortListObject sobj1 = objs.get(i);
                //String key = "test" + CommonTools.getComparableString(i + 2, 5);
                //Assert.assertEquals(sobj1.getKey(), key);
                //Assert.assertEquals(sobj1.getObjid(), "obj1");
                list.delete(sobj1);
            }
            System.out.println("list size : " + list.getSize());
        }
        List<SortListObject> objs = list.getRange(0, -1);
        System.out.println("list size : " + list.getSize());
        for (int i = 0; i < objs.size() - 10; i++) {
            SortListObject sobj1 = objs.get(i);
            //String key = "test" + CommonTools.getComparableString(i + 2, 5);
            //Assert.assertEquals(sobj1.getKey(), key);
            //Assert.assertEquals(sobj1.getObjid(), "obj1");
            list.delete(sobj1);
        }
        // Assert.assertEquals(4000, list.getSize());

        System.out.println("list size : " + list.getSize());
        long end = System.currentTimeMillis();


        logger.severe("---------------------------------listTest : " + (end - begin) + "ms");

    }

    ////Test
    public static void testIsExists() throws Exception {
        IListFactory listFactory = pigeonStoreEngine.getListFactory();

        String listId = "test" + System.currentTimeMillis();
        ISortList list = listFactory.getList(listId, true);
        SortListObject srObj = new SortListObject();
        srObj.setObjid("obj1");
        for (int i = 0; i < 1200; i++) {
            String key = "test" + CommonTools.getComparableString(i + 2, 5);
            srObj.setKey(key);
            list.add(srObj);
        }


        List<SortListObject> objs = list.getRange(0, -1);
        //Assert.assertEquals(1200, objs.size());
        for (int i = 0; i < objs.size(); i++) {
            SortListObject sobj1 = objs.get(i);
            String key = "test" + CommonTools.getComparableString(i + 2, 5);
            //Assert.assertEquals(sobj1.getKey(), key);
            // Assert.assertEquals(sobj1.getObjid(), "obj1");
            //Assert.assertEquals(true, list.isExists(sobj1.getKey(), sobj1.getObjid()));
            list.delete(sobj1);
            //Assert.assertEquals(false, list.isExists(sobj1.getKey(), sobj1.getObjid()));
        }
        //Assert.assertEquals(0, list.getSize());
    }

    ////Test
    public static void getSortListObjectTest() throws Exception {
        IListFactory listFactory = pigeonStoreEngine.getListFactory();

        String listId = "test" + System.currentTimeMillis();
        ISortList list = listFactory.getList(listId, true);
        SortListObject srObj = new SortListObject();
        srObj.setObjid("obj1");
        for (int i = 0; i < 625000; i++) {
            String key = "test" + CommonTools.getComparableString(i + 2, 5);
            srObj.setKey(key);
            list.add(srObj);
        }

        List<SortListObject> objs = list.getRange(0, -1);
        //Assert.assertEquals(1200, objs.size());
        for (int i = 0; i < objs.size(); i++) {
            SortListObject sobj1 = objs.get(i);
            String key = "test" + CommonTools.getComparableString(i + 2, 5);
            // Assert.assertEquals(sobj1.getKey(), key);
            // Assert.assertEquals(sobj1.getObjid(), "obj1");
            // Assert.assertEquals(true, list.isExists(sobj1.getKey(), sobj1.getObjid()));
            list.delete(sobj1);
            // Assert.assertEquals(false, list.isExists(sobj1.getKey(), sobj1.getObjid()));
        }
    }


    public static void testAtomCreateAndSet000() throws Exception {
        IIntegerAtom atom = pigeonStoreEngine.getAtom();
        for (int i = 0; i < 100; i++) {
            String atomId = "atom" + i;
            atom.createAndSet(atomId, 1000000);
            long v = atom.get(atomId);
            // Assert.assertEquals(v, 1000000);
        }
    }

    public static void testAtom000() throws Exception {
        IIntegerAtom atom = pigeonStoreEngine.getAtom();
        Vector<Integer> vec = new Vector<Integer>(10000);
        for (int j = 0; j < 10000; j++) {
            int i = RandomUtils.nextInt();
            if (i < 0) {
                i = -i;
            }
            int n = i % 100;
            vec.add(n);
        }
        for (Integer ii : vec) {
            String atomId = "atom" + ii;
            atom.createAndSet(atomId, 0);
        }
        for (Integer ii : vec) {
            String atomId = "atom" + ii;
            atom.greaterAndInc(atomId, 0, 123);
        }
        for (Integer ii : vec) {
            String atomId = "atom" + ii;
            atom.lessAndInc(atomId, 10000000, -123);
        }
    }


    ////Test
    public static void testAtomCreateAndSet() throws Exception {
        IIntegerAtom atom = pigeonStoreEngine.getAtom();
        String atomId = "atom" + System.currentTimeMillis();
        atom.createAndSet(atomId, 10);

        long v = atom.get(atomId);
        Assert.assertEquals(v, 10);
    }

    public static void test_atom_state() throws Exception {
        IIntegerAtom atom = pigeonStoreEngine.getAtom();
        String atomId = "atom" + System.currentTimeMillis();

        for (int i = 0; i < 1000; i++) {
            try {
                atom.createAndSet(atomId + i, 10);
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                long v = atom.get(atomId + i);
                Assert.assertEquals(v, 10);
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                long r = atom.greaterAndIncReturnLong(atomId + i, 9, 1);
                Assert.assertEquals(11, r);
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                long v = atom.get(atomId + i);
                Assert.assertEquals(v, 11);
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                long r = atom.greaterAndIncReturnLong(atomId + i, 12, 1);
                Assert.assertEquals(11, r);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    ////Test
    public static void testGreaterAndInc() throws Exception {
        IIntegerAtom atom = pigeonStoreEngine.getAtom();
        String atomId = "atom" + System.currentTimeMillis();
        atom.createAndSet(atomId, 10);

        long v = atom.get(atomId);
        Assert.assertEquals(v, 10);

        long r = atom.greaterAndIncReturnLong(atomId, 9, 1);
        Assert.assertEquals(11, r);

        v = atom.get(atomId);
        Assert.assertEquals(v, 11);

        r = atom.greaterAndIncReturnLong(atomId, 12, 1);
        Assert.assertEquals(11, r);

    }

    ////Test
    public static void testGetAtoms() throws Exception {
        IIntegerAtom atom = pigeonStoreEngine.getAtom();

        List<String> atomIds = new Vector<String>();
        for (int i = 0; i < 100; i++) {
            String atomId = "atom" + System.currentTimeMillis() + "_" + i;
            atom.createAndSet(atomId, i);
            atomIds.add(atomId);
        }

        List<Long> values = atom.getAtoms(atomIds);
        for (int i = 0; i < 100; i++) {
            long v = values.get(i);
            Assert.assertEquals((int) v, i);
        }
    }

    public static void testGetGetIds3() throws Exception {
        IIDGenerator idgenerator = pigeonStoreEngine.getIdGenerator();
        int count = 200000;
        Map m = new HashMap();
        long oi = -1;
        long ts = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            int n = i / 10000;
            long id = idgenerator.getId("o" + n);
            /*if (m.containsKey("" + id)) {
                throw new Exception("duplicated id:" + id);
            }
            m.put("" + id, id);*/
            if (oi != -1 && id < oi) {
                System.out.println("oi = " + oi + " id = " + id);
            }
            oi = id;
        }
        long te = System.currentTimeMillis();
        System.out.println(" id thread over! ms = " + (te - ts));
    }

    ////Test
    public static void testGetGetIds() throws Exception {
        IIDGenerator idgenerator = pigeonStoreEngine.getIdGenerator();
        int count = 20000;
        Map m = new HashMap();
        for (int i = 0; i < count; i++) {

            long id = idgenerator.getId("o");
            if (m.containsKey("" + id)) {
                throw new Exception("duplicated id:" + id);
            }
            m.put("" + id, id);
        }
    }

    public static void testGetGetIdsState() throws Exception {
        IIDGenerator idgenerator = pigeonStoreEngine.getIdGenerator();
        int count = 20000;
        Map m = new HashMap();
        for (int i = 0; i < count; i++) {
            try {
                long id = idgenerator.getId("o");
                if (m.containsKey("" + id)) {
                    throw new Exception("duplicated id:" + id);
                }
                m.put("" + id, id);
                System.out.println("o:" + id);
                Thread.sleep(200);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    ////Test
    public static void testAddFile() throws Exception {
        File f = new File("pigeon-server/src/test/resources/pigeonclient.conf");
        //fileSystem.addFile(f,"pigeonclient.conf");
        long currentTime = System.currentTimeMillis();
        String s = "" + currentTime + "_testAddFile";
        fileSystem.addBytes(s.getBytes("UTF-8"), s);
    }

    ////Test
    public static void testGenFile() throws Exception {
        File f = new File("pigeon-server/src/test/resources/170000.JPG");
        f = f.getAbsoluteFile();
        //fileSystem.addFile(f,"pigeonclient.conf");
        long currentTime = System.currentTimeMillis();
        String s = "" + currentTime + "_testAddFile";
        String fileId = fileSystem.addFile(f, "170000.JPG");
        if (fileId == null) {
            throw new Exception("addFile return null." + f.getAbsolutePath());
        }
        fileSystem.genRelatedFile(fileId, "100X100");
    }

    ////Test
    public static void testDeleteFile() throws Exception {
        File f = new File("pigeon-server/src/test/resources/020.jpg");
        f = f.getAbsoluteFile();
        //fileSystem.addFile(f,"pigeonclient.conf");
        long currentTime = System.currentTimeMillis();
        String s = "" + currentTime + "_testAddFile";
        String fileId = fileSystem.addFile(f, "020.jpg");
        if (fileId == null) {
            throw new Exception("addFile return null." + f.getAbsolutePath());
        }
        fileSystem.genRelatedFile(fileId, "100X100");

        fileSystem.delete(fileId);
        fileSystem.delete(fileId);


    }


    //Test
    public void stopTest() throws IOException {
        PigeonListener.stop();
    }

    public void testCheckExist() {
    }

    public void testGetExternalUrl() {
    }

    public void testGetInternalUrl() {
    }

    ////Test
    void testNothing() {
        try {
            Thread.sleep(1000 * 10 * 6);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }


}
