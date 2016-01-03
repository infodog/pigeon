package net.xinshi.pigeon.backup;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.adapter.impl.DistributedPigeonEngine;
import net.xinshi.pigeon.backup.util.DownloadAppendToFile;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-9-18
 * Time: 上午11:15
 * To change this template use File | Settings | File Templates.
 */

public class PigeonBackup {

    private static Map<String, List<String>> backup(String key, IPigeonStoreEngine pigeonStoreEngine) throws Exception {
        if (pigeonStoreEngine == null) {
            throw new Exception("pigeonStoreEngine == null");
        }
        if (!(pigeonStoreEngine instanceof DistributedPigeonEngine)) {
            throw new Exception("pigeonStoreEngine != DistributedPigeonEngine");
        }
        Map<String, List<String>> map = ((DistributedPigeonEngine) pigeonStoreEngine).backup(key);
        for (String e : map.keySet()) {
            List<String> list = map.get(e);
            if (list != null) {
                for (String v : list) {
                    // System.out.println("download url = " + e + " : " + v);
                }
            }
        }
        return map;
    }

    public static synchronized String backup(String key, String SaveDir, IPigeonStoreEngine pigeonStoreEngine) throws Exception {
        File f = new File(SaveDir);
        if (!f.exists()) {
            f.mkdirs();
        }
        if (!f.exists() || !f.isDirectory()) {
            throw new Exception("SaveDir : " + SaveDir + ", not exists or not a directory");
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSSS");
        String str = sdf.format(new Date());
        if (key != null) {
            str += "_" + key;
        }
        str = SaveDir + "/" + str;
        f = new File(str);
        if (f.exists()) {
            throw new Exception("save dir exists : " + str);
        }
        f.mkdirs();
        if (!f.exists()) {
            throw new Exception("can't create save dir : " + str);
        }
        str = f.getAbsolutePath();
        Map<String, List<String>> map = PigeonBackup.backup(key, pigeonStoreEngine);
        for (String e : map.keySet()) {
            List<String> list = map.get(e);
            if (list != null) {
                for (String v : list) {
                    DownloadAppendToFile.downloadAppendToFile(str + "/" + e + ".txt", v);
                }
            }
        }
        System.out.println("ClientBackup save to : " + str);
        return str;
    }


    public static void main(String[] args) throws Exception {
        String mid = null;
        if (args.length < 1) {
            System.out.println("PigeonBackup command(backup|verify) [mid]");
            return;
        }
        if (args[0].compareToIgnoreCase("backup") == 0) {
            if (args.length == 2) {
                mid = args[1];
            }
            File f = new File("./pigeon-server/src/test/resources/pigeonnodes3.conf");
            IPigeonStoreEngine pigeonStoreEngine = new DistributedPigeonEngine(f.getAbsolutePath());
            PigeonBackup.backup(mid, "./data", pigeonStoreEngine);
        } else if (args[0].compareToIgnoreCase("verify") == 0) {
            VerifyHash.handle();
        } else {
            System.out.println("bad command = " + args[0]);
        }
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
