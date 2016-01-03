package net.xinshi.pigeon.pigeonserver;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.adapter.impl.SingleServerEngine;
import net.xinshi.pigeon.atom.IIntegerAtom;
import net.xinshi.pigeon.filesystem.impl.PigeonFileSystem;
import net.xinshi.pigeon.flexobject.FlexObjectEntry;
import net.xinshi.pigeon.idgenerator.IIDGenerator;
import net.xinshi.pigeon.list.IListFactory;
import net.xinshi.pigeon.list.ISortList;
import net.xinshi.pigeon.list.SortListObject;
import net.xinshi.pigeon.server.standalongserver.Constants;
import net.xinshi.pigeon.server.standalongserver.PigeonListener;
import net.xinshi.pigeon.util.CommonTools;
import org.apache.commons.lang.StringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: mac
 * Date: 11-11-6
 * Time: ����2:55
 * To change this template use File | Settings | File Templates.
 */
public class PigeonServerTest {


    IPigeonStoreEngine pigeonStoreEngine;
    PigeonFileSystem fileSystem;
    Logger logger = Logger.getLogger("pigeonServerTest");

    @BeforeTest
    public void setup() throws Exception {
        logger.log(Level.INFO, "setup");
        startServers();
        createPigeonClient();
        createPigeonFileSystem();
    }

    public void startServers() throws Exception {
        File f = new File("src/test/resources/pigeonservertest.conf");
        PigeonListener.startServers(Integer.parseInt(Constants.defaultPort),
                Integer.parseInt(Constants.defaultNettyPort),
                new String[]{f.getAbsolutePath()});
    }


    public void createPigeonClient() throws Exception {
        File f = new File("src/test/resources/pigeonclient.conf");
        pigeonStoreEngine = new SingleServerEngine(f.getAbsolutePath());
    }

    public void createPigeonFileSystem() throws Exception {
        fileSystem = new PigeonFileSystem("src/test/resources/pigeonclient.conf", "src/test/resources/pigeonfilesystem.conf");
    }

    ////Test
    public void flexObjectSaveTest() throws Exception {
        try {
            String content = "111111";
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
    public void flexObjectGetContentsTest() throws Exception {
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

    ////Test
    public void flexObjectSaveLargeStringTest() throws Exception {
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

    @Test
    public void flexObjectAddLargeStringTest() throws Exception {
        String s = StringUtils.repeat("adcvc", 1024);
        String name = "largeObj" + System.currentTimeMillis();
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            pigeonStoreEngine.getFlexObjectFactory().addContent(name + "_" + i, s);
        }
        long end = System.currentTimeMillis();


        logger.severe("---------------------------------flexObjectSaveLargeStringTest : " + (end - begin) + "ms");

    }

    ////Test
    public void saveFlexObjectsTest() throws Exception {
        String s = StringUtils.repeat("adcvc", 1024);
        String name = "largeObj" + System.currentTimeMillis();
        long begin = System.currentTimeMillis();

        List<FlexObjectEntry> objs = new Vector<FlexObjectEntry>();


        for (int i = 0; i < 10000; i++) {
            FlexObjectEntry obj = new FlexObjectEntry();
            obj.setName(name + "_" + i);
            obj.setAdd(true);
            obj.setCompressed(true);
            obj.setString(true);
            obj.setBytesContent(CommonTools.zip(s.getBytes("UTF-8")));
            objs.add(obj);

        }
        pigeonStoreEngine.getFlexObjectFactory().saveFlexObjects(objs);
        long end = System.currentTimeMillis();


        logger.log(Level.WARNING, "flexObjectSaveLargeStringTest : " + (end - begin) + "ms");
    }

    ////Test
    public void testDeleteObject() throws Exception {
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
    public void testAddListObject() throws Exception {
        IListFactory listFactory = pigeonStoreEngine.getListFactory();

        String listId = "test" + System.currentTimeMillis();
        ISortList list = listFactory.getList(listId, true);
        SortListObject srObj = new SortListObject("test1", "obj1");
        list.add(srObj);


        boolean result = list.add(srObj);
        if (result != false) {
            throw new Exception("error. duplicated obj.");
        }

        Assert.assertEquals(list.getSize(), 1);

        srObj.setKey("test2");
        list.add(srObj);

        Assert.assertEquals(list.getSize(), 2);


    }

    ////Test
    public void testGetRange() throws Exception {
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
        Assert.assertEquals(1200, objs.size());
        for (int i = 0; i < objs.size(); i++) {
            SortListObject sobj1 = objs.get(i);
            String key = "test" + CommonTools.getComparableString(i + 2, 5);
            Assert.assertEquals(sobj1.getKey(), key);
            Assert.assertEquals(sobj1.getObjid(), "obj1");
        }

    }

    ////Test
    public void testDelete() throws Exception {
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
        Assert.assertEquals(1200, objs.size());
        for (int i = 0; i < objs.size(); i++) {
            SortListObject sobj1 = objs.get(i);
            String key = "test" + CommonTools.getComparableString(i + 2, 5);
            Assert.assertEquals(sobj1.getKey(), key);
            Assert.assertEquals(sobj1.getObjid(), "obj1");
            list.delete(sobj1);
        }
        Assert.assertEquals(0, list.getSize());
    }

    ////Test
    public void testIsExists() throws Exception {
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
        Assert.assertEquals(1200, objs.size());
        for (int i = 0; i < objs.size(); i++) {
            SortListObject sobj1 = objs.get(i);
            String key = "test" + CommonTools.getComparableString(i + 2, 5);
            Assert.assertEquals(sobj1.getKey(), key);
            Assert.assertEquals(sobj1.getObjid(), "obj1");
            Assert.assertEquals(true, list.isExists(sobj1.getKey(), sobj1.getObjid()));
            list.delete(sobj1);
            Assert.assertEquals(false, list.isExists(sobj1.getKey(), sobj1.getObjid()));
        }
        Assert.assertEquals(0, list.getSize());
    }

    ////Test
    public void getSortListObjectTest() throws Exception {
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
        Assert.assertEquals(1200, objs.size());
        for (int i = 0; i < objs.size(); i++) {
            SortListObject sobj1 = objs.get(i);
            String key = "test" + CommonTools.getComparableString(i + 2, 5);
            Assert.assertEquals(sobj1.getKey(), key);
            Assert.assertEquals(sobj1.getObjid(), "obj1");
            Assert.assertEquals(true, list.isExists(sobj1.getKey(), sobj1.getObjid()));
            list.delete(sobj1);
            Assert.assertEquals(false, list.isExists(sobj1.getKey(), sobj1.getObjid()));
        }
    }

    ////Test
    public void testAtomCreateAndSet() throws Exception {
        IIntegerAtom atom = pigeonStoreEngine.getAtom();
        String atomId = "atom" + System.currentTimeMillis();
        atom.createAndSet(atomId, 10);

        long v = atom.get(atomId);
        Assert.assertEquals(v, 10);
    }

    ////Test
    public void testGreaterAndInc() throws Exception {
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
    public void testGetAtoms() throws Exception {
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

    ////Test
    public void testGetGetIds() throws Exception {
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


    @Test
    public void testAddFile() throws Exception {
        File f = new File("src/test/resources/pigeonclient.conf");
        //fileSystem.addFile(f,"pigeonclient.conf");
        long currentTime = System.currentTimeMillis();
        String s = "" + currentTime + "_testAddFile";
        fileSystem.addBytes(s.getBytes("UTF-8"), s);
    }


    public void testGenFile() throws Exception {
        File f = new File("src/test/resources/170000.JPG");
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

    @Test
    public void testDeleteFile() throws Exception {
        File f = new File("src/test/resources/020.jpg");
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

    @Test
    void testNothing() {
        try {
            Thread.sleep(1000 * 10 * 6);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }


}
