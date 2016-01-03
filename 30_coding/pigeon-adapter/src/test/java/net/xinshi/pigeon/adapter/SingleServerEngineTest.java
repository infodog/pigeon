package net.xinshi.pigeon.adapter;

import org.testng.annotations.Test;

import java.io.File;

/**
 * Created by IntelliJ IDEA.
 * User: mac
 * Date: 11-11-6
 * Time: 下午5:01
 * To change this template use File | Settings | File Templates.
 */
public class SingleServerEngineTest {

    @Test
    public static void testCreateServerEngine() throws Exception {
        File f = new File("src/configs/pigeonclient.conf");
        //IPigeonStoreEngine pigeonStoreEngine = new SingleServerEngine(f.getAbsolutePath());
    }
}
