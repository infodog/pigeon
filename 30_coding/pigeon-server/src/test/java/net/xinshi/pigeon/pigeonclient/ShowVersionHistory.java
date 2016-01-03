package net.xinshi.pigeon.pigeonclient;

import net.xinshi.pigeon.persistence.VersionHistory;
import net.xinshi.pigeon.persistence.VersionHistoryLogger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-29
 * Time: 上午11:15
 * To change this template use File | Settings | File Templates.
 */

public class ShowVersionHistory {


    public static void main(String[] args) throws Exception {
        String fn = "c:\\0.bin";
        // File f = new File(args[0]);
        File f = new File(fn);
        FileInputStream is = new FileInputStream(f);
        Logger logger = Logger.getLogger("showversions");
        FileOutputStream os = new FileOutputStream("c:\\ver1.txt");
        long pre = -1;
        System.out.println("now version = " + pre);
        while (true) {
            VersionHistory vh = VersionHistoryLogger.getVersionHistoryFromFIS(is);
            if (vh == null) {
                break;
            }
            // logger.warning("" + vh.getVersion());
            if (pre != -1 && pre + 1 != vh.getVersion()) {
                System.out.println("version wrong ... now = " + pre + ", next = " + vh.getVersion());
            }
            pre = vh.getVersion();
            System.out.println(vh.getVersion());
            os.write(("" + vh.getVersion() + "\t").getBytes());
            os.write(vh.getData());
            os.write("\r\n".getBytes());
        }
        System.out.println("now version = " + pre);
        is.close();
        os.close();
    }
}

