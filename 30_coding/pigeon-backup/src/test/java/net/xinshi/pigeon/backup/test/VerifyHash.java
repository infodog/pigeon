package net.xinshi.pigeon.backup.test;

import net.xinshi.pigeon.distributed.util.DefaultHashGenerator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-9-19
 * Time: 下午5:17
 * To change this template use File | Settings | File Templates.
 */

public class VerifyHash {

    static String hexToString(String strValue) throws Exception {
        int len = strValue.length();
        if (len == 0) {
            return "";
        }
        if (len % 2 != 0) {
            throw new Exception("bad hex string : " + strValue);
        }
        byte buff[] = strValue.getBytes();
        byte byteData[] = new byte[len / 2];
        int j = 0;
        for (int i = 0; i < len; i += 2) {
            byte b1 = buff[i];
            byte b2 = buff[i + 1];
            if (b1 >= '0' && b1 <= '9') {
                b1 -= '0';
            } else if (b1 >= 'a' && b1 <= 'f') {
                b1 -= 'a' - 10;
            } else if (b1 >= 'A' && b1 <= 'F') {
                b1 -= 'A' - 10;
            } else {
                throw new Exception("bad hex string : " + strValue);
            }
            if (b2 >= '0' && b2 <= '9') {
                b2 -= '0';
            } else if (b2 >= 'a' && b2 <= 'f') {
                b2 -= 'a' - 10;
            } else if (b2 >= 'A' && b2 <= 'F') {
                b2 -= 'A' - 10;
            } else {
                throw new Exception("bad hex string : " + strValue);
            }
            byteData[j++] = (byte) (b1 << 4 | b2);
        }
        return new String(byteData, "UTF-8");
    }

    static void verifyFile(File f, FileOutputStream fos) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(f));
        String line;
        int count = 0;
        int ignore = 0;
        int less = -1;
        while (true) {
            if ((line = br.readLine()) == null) {
                break;
            }
            line.replace("\r", "");
            line.replace("\n", "");
            String[] parts = line.split("\t");
            if (parts.length == 1) {
                System.out.println("warnning : line = " + line);
                if (line.indexOf("172.16.1.5") > 0) {
                    less = 0;
                } else if (line.indexOf("172.16.1.4") > 0) {
                    less = 1;
                } else {
                    less = -1;
                }
                ++ignore;
                continue;
            }
            if (less == -1) {
                throw new Exception("bad range");
            }
            String name = hexToString(parts[0]);
            int hash = DefaultHashGenerator.hash(name);
            if ((less == 1 && hash <= 0) || (less == 0 && hash > 0)) {
                fos.write((line + "\n").getBytes());
                ++count;
            } else {
                // System.out.println("warnning : bad hash line = " + line);
                ++ignore;
            }
        }
        System.out.println("count = " + count + ", ignore count = " + ignore);
    }

    public static void main(String[] args) throws Exception {
        {
            File f = new File("D:\\sz\\201209191700050614\\flexobject.txt");
            FileOutputStream fos = new FileOutputStream(f.getAbsoluteFile() + ".out");
            verifyFile(f, fos);
            fos.close();
        }
        {
            File f = new File("D:\\sz\\201209191700050614\\list.txt");
            FileOutputStream fos = new FileOutputStream(f.getAbsoluteFile() + ".out");
            verifyFile(f, fos);
            fos.close();
        }
        {
            File f = new File("D:\\sz\\201209191700050614\\atom.txt");
            FileOutputStream fos = new FileOutputStream(f.getAbsoluteFile() + ".out");
            verifyFile(f, fos);
            fos.close();
        }
        {
            File f = new File("D:\\sz\\201209191700050614\\idserver.txt");
            FileOutputStream fos = new FileOutputStream(f.getAbsoluteFile() + ".out");
            verifyFile(f, fos);
            fos.close();
        }
    }

}

