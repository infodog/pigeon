package net.xinshi.pigeon.sharebin;

import net.xinshi.pigeon.flexobject.FlexObjectEntry;
import net.xinshi.pigeon.persistence.VersionHistoryLogger;
import net.xinshi.pigeon.util.CommonTools;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-6-19
 * Time: 上午11:21
 * To change this template use File | Settings | File Templates.
 */

public class ShowShareBin {

    public static void showFlexObject(String infile, String outfile) throws Exception {
        FileInputStream fis = new FileInputStream(infile);
        FileOutputStream fos = new FileOutputStream(outfile);
        try {
            VersionHistoryLogger verLogger = new VersionHistoryLogger();
            while (true) {
                InputStream mis = null;
                try {
                    mis = verLogger.getInputStreamFromVersionHistoryFile(fis);
                    if (mis == null) {
                        break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
                FlexObjectEntry entry = CommonTools.readEntry(mis);
                if (entry == null) {
                    break;
                }
                String s = entry.getName() + "\t" + entry.getContent() + "\r\n";
                fos.write(s.getBytes());
            }
            fis.close();
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String argv[]) throws Exception {
        showFlexObject("c:\\00.bin", "c:\\00.txt");
    }

}
