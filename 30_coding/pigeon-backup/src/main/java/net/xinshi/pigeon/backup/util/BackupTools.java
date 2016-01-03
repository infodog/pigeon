package net.xinshi.pigeon.backup.util;

import net.xinshi.pigeon.netty.common.Constants;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-9-5
 * Time: 上午11:18
 * To change this template use File | Settings | File Templates.
 */

public class BackupTools {

    public static synchronized String buildTempFileName(short type, String key) {
        String t = "";
        switch (type) {
            case Constants.FLEXOBJECT_TYPE:
                t = "o";
                break;
            case Constants.LIST_TYPE:
                t = "l";
                break;
            case Constants.ATOM_TYPE:
                t = "a";
                break;
            case Constants.ID_TYPE:
                t = "i";
                break;
        }
        String m = "_";
        if (key != null) {
            m += key + "_";
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSSS");
        String str = sdf.format(new Date());
        String temp = t + m + str + ".txt";
        return temp;
    }

    public static synchronized String getBackupPath() {
        File f = new File("../backup");
        if (!f.exists()) {
            f.mkdirs();
        }
        return f.getAbsolutePath();
    }

}

