package net.xinshi.pigeon.test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: WPF
 * Date: 13-3-19
 * Time: 上午11:07
 * To change this template use File | Settings | File Templates.
 */
public class test {

    static class bean {
        public String x;
        public String y;
    }

    static String[] table = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F"};

    static String bytes2hex(byte[] array) {
        StringBuilder sb = new StringBuilder(array.length * 2);
        for (int i = 0; i < array.length; i++) {
            sb.append(table[(array[i] >> 4) & 0xF]);
            sb.append(table[array[i] & 0xF]);
        }
        return sb.toString();
    }


    public static void main(String argv[]) {
        try {

            String tt = "a1b2c3你好，软件！！1a2b3c";

            String hex = bytes2hex(tt.getBytes());


            List<String> ls = new ArrayList<String>();
            ls.add("d:\\");
            String as = net.xinshi.pigeon.util.ClientTools.doInclude("//#import a.jsx", ls);

            File f = new File("../a.txt");
            String fn = f.getAbsolutePath();

            fn = f.getCanonicalPath();

            bean b1 = new bean();
            b1.x = "123456";
            b1.y = "abcdef";

            bean b2 = new bean();
            b2.x = b1.x;
            b2.y = b1.y;

            b1.x = "111111";

            System.getProperties().list(System.out);

            String p = System.getProperty("file.encoding");
            String u = "123你好123";
            byte[] ub = u.getBytes();

            System.setProperty("file.encoding", "GBK");

            String p2 = System.getProperty("file.encoding");

            byte[] ub2 = u.getBytes();

            String g = new String(u.getBytes("UTF-8"), "GBK");
            byte[] gb = g.getBytes();
            int i = 0;
            i++;
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}