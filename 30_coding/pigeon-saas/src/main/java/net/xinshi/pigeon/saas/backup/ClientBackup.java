package net.xinshi.pigeon.saas.backup;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.adapter.StaticPigeonEngine;
import net.xinshi.pigeon.adapter.impl.DistributedPigeonEngine;
import net.xinshi.pigeon.adapter.impl.NormalPigeonEngine;
import net.xinshi.pigeon.backup.util.DownloadAppendToFile;
import net.xinshi.pigeon.saas.SaasPigeonEngine;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-9-6
 * Time: 上午9:29
 * To change this template use File | Settings | File Templates.
 */

public class ClientBackup {

    private static Map<String, List<String>> backup(String key, IPigeonStoreEngine pigeonStoreEngine) throws Exception {
        if (pigeonStoreEngine == null) {
            if (StaticPigeonEngine.pigeon != null && StaticPigeonEngine.pigeon instanceof NormalPigeonEngine) {
                pigeonStoreEngine = ((NormalPigeonEngine) StaticPigeonEngine.pigeon).getPigeonStoreEngine();
            } else if (StaticPigeonEngine.pigeon instanceof SaasPigeonEngine) {
                if (((SaasPigeonEngine) StaticPigeonEngine.pigeon).getRawPigeonEngine() instanceof NormalPigeonEngine) {
                    pigeonStoreEngine = ((NormalPigeonEngine) ((SaasPigeonEngine) StaticPigeonEngine.pigeon).getRawPigeonEngine()).getPigeonStoreEngine();
                }
            }
        }
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
        Map<String, List<String>> map = ClientBackup.backup(key, pigeonStoreEngine);
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

    public static synchronized String backup(String key, String SaveDir) throws Exception {
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
        Map<String, List<String>> map = ClientBackup.backup(key, (IPigeonStoreEngine) null);
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

}

