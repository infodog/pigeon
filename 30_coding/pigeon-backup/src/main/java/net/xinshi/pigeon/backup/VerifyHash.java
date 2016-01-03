package net.xinshi.pigeon.backup;

import net.xinshi.pigeon.distributed.util.DefaultHashGenerator;
import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-9-19
 * Time: 下午5:17
 * To change this template use File | Settings | File Templates.
 */

public class VerifyHash {

    static class range {
        public int min;
        public int max;

        public range(int min, int max) {
            this.min = min;
            this.max = max;
        }
    }

    static String backupDataDir;
    static Map<String, List<range>> ipRange = new HashMap<String, List<range>>();

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
        List<range> listRange = null;
        while (true) {
            if ((line = br.readLine()) == null) {
                break;
            }
            line.replace("\r", "");
            line.replace("\n", "");
            String[] parts = line.split("\t");
            if (parts.length == 1) {
                System.out.println("warnning : line = " + line);
                int s = line.indexOf("://");
                if (s < 0) {
                    throw new Exception("bad prefix = " + line);
                }
                s += 3;
                int m = line.indexOf(":", s);
                int x = line.indexOf("/", s);
                int e = m;
                if (m < 0 || m > x) {
                    e = x;
                }
                listRange = ipRange.get(line.substring(s, e));
                if (listRange == null) {
                    throw new Exception("bad listRange = " + line);
                }
                ++ignore;
                continue;
            }
            String name = hexToString(parts[0]);
            int hash = DefaultHashGenerator.hash(name);
            boolean hit = false;
            for (range rg : listRange) {
                if (hash >= rg.min && hash <= rg.max) {
                    hit = true;
                    break;
                }
            }
            if (hit) {
                fos.write((line + "\n").getBytes());
                ++count;
            } else {
                ++ignore;
            }
        }
        System.out.println("count = " + count + ", ignore count = " + ignore);
    }

    public static void init() throws Exception {
        File f = new File("./verifyhash.conf");
        FileInputStream is = new FileInputStream(f);
        byte[] b = new byte[(int) f.length()];
        is.read(b);
        is.close();
        String s = new String(b, "UTF-8");
        String info = StringUtils.trim(s);
        JSONObject jo = new JSONObject(info);
        backupDataDir = jo.optString("backupDataDir");
        if (backupDataDir.trim().length() == 0) {
            System.out.println("error : backupDataDir = " + backupDataDir);
            throw new Exception("exit");
        }
        backupDataDir = (new File(backupDataDir)).getAbsolutePath();
        System.out.println("backupDataDir = " + backupDataDir);
        JSONArray jsa = jo.getJSONArray("range");
        for (int i = 0; i < jsa.length(); i++) {
            JSONObject jot = jsa.getJSONObject(i);
            Iterator iterator = jot.keys();
            while (iterator.hasNext()) {
                String key = (String) iterator.next();
                String value = jot.optString(key);
                String[] parts = value.split("~");
                if (parts.length != 2) {
                    System.out.println("error : range = " + value);
                    throw new Exception("exit");
                }
                int leftBoundary = Integer.parseInt(parts[0]);
                int rightBoundary = Integer.parseInt(parts[1]);
                List<range> listRange = ipRange.get(key);
                if (listRange == null) {
                    listRange = new ArrayList<range>();
                    ipRange.put(key, listRange);
                }
                listRange.add(new range(leftBoundary, rightBoundary));
                System.out.println("host = " + key + ", min = " + leftBoundary + ", max = " + rightBoundary);
            }
        }
    }

    public static void handle() throws Exception {
        init();
        {
            File f = new File(backupDataDir + "/flexobject.txt");
            FileOutputStream fos = new FileOutputStream(f.getAbsoluteFile() + ".out");
            verifyFile(f, fos);
            fos.close();
        }
        {
            File f = new File(backupDataDir + "/list.txt");
            FileOutputStream fos = new FileOutputStream(f.getAbsoluteFile() + ".out");
            verifyFile(f, fos);
            fos.close();
        }
        {
            File f = new File(backupDataDir + "/atom.txt");
            FileOutputStream fos = new FileOutputStream(f.getAbsoluteFile() + ".out");
            verifyFile(f, fos);
            fos.close();
        }
        {
            File f = new File(backupDataDir + "/idserver.txt");
            FileOutputStream fos = new FileOutputStream(f.getAbsoluteFile() + ".out");
            verifyFile(f, fos);
            fos.close();
        }
    }

}

