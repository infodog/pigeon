package net.xinshi.pigeon.clienttools;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-7-19
 * Time: 下午3:32
 * To change this template use File | Settings | File Templates.
 */

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.adapter.impl.DistributedPigeonEngine;
import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.util.Vector;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-6-8
 * Time: 下午3:22
 * To change this template use File | Settings | File Templates.
 */

public class PigeonClientTools {
    static String pigeonStoreConfigFile;
    static String pigeonScriptFile;
    static IPigeonStoreEngine pigeonStoreEngine;


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
        pigeonScriptFile = jo.optString("pigeonScriptFile");
        if (pigeonScriptFile.length() < 1) {
            throw new Exception("config file bad ... ");
        }
        f = new File(pigeonScriptFile);
        System.out.println("pigeonScriptFile = " + f.getAbsolutePath());
        f = new File(pigeonStoreConfigFile);
        System.out.println("pigeonStoreConfigFile = " + f.getAbsolutePath());
        pigeonStoreEngine = new DistributedPigeonEngine(f.getAbsolutePath());
    }


    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.out.println("useage : java PigeonClientTools pigeonclienttools.conf");
            return;
        }
        String config = args[0];

        // String config = "pigeon-import-data/resources/pigeonimportdata.conf";

        init(config);

        long st = System.currentTimeMillis();

        File f = new File(pigeonScriptFile);
        Vector results = PigeonScriptTools.execAll(pigeonStoreEngine, f.getAbsoluteFile());

        for (Object result : results) {
            System.out.println(result);
        }

        long et = System.currentTimeMillis();

        System.out.println("PigeonClientTools all ...... time ms = " + (et - st));

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
