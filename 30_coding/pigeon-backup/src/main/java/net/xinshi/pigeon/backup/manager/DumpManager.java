package net.xinshi.pigeon.backup.manager;

import net.xinshi.pigeon.backup.util.BackupTools;

import java.io.File;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-9-5
 * Time: 上午11:38
 * To change this template use File | Settings | File Templates.
 */
public class DumpManager {

    public static synchronized String buildLogFile(short type, String key, String path) throws Exception {
        String temp = BackupTools.buildTempFileName(type, key);
        String file = path + "/" + temp;
        File f = new File(file);
        if (f.exists()) {
            throw new Exception(f.getAbsolutePath() + " : file exist");
        }
        return f.getAbsolutePath();
    }

}
