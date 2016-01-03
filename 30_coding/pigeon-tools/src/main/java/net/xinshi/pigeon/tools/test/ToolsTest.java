package net.xinshi.pigeon.tools.test;

import net.xinshi.pigeon.distributed.util.DefaultHashGenerator;

import java.io.File;
import java.io.FileInputStream;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-4-12
 * Time: 上午11:31
 * To change this template use File | Settings | File Templates.
 */

public class ToolsTest {

    static String hexToString0(String strValue) throws Exception {
        int len = strValue.length();
        if (len % 2 != 0) {
            throw new Exception("bad hex string : " + strValue);
        }
        len /= 2;
        String strReturn = "";
        String strHex = "";
        int intHex = 0;
        byte byteData[] = new byte[len];
        try {
            for (int i = 0; i < len; i++) {
                strHex = strValue.substring(0, 2);
                strValue = strValue.substring(2);
                intHex = Integer.parseInt(strHex, 16);
                if (intHex > 128) {
                    intHex = intHex - 256;
                }
                byteData[i] = (byte) intHex;
            }
            strReturn = new String(byteData, "UTF-8");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return strReturn;
    }

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

    public static void main(String[] args) throws Exception {

        String t = "7B226964223A22776F726B73706163655F73657474696E67222C2269636F6E223A22636F6C49636F6E222C226E616D65223A22E9858DE7BDAE222C226D6F64756C6573223A226D6F64756C655F44656661756C742C6D6F64756C655F44796E61417474722C6D6F64756C655F436F6C756D6E4D67742C6D6F64756C655F737973617267756D656E742C6D6F64756C655F526567696F6E4D67742C6D6F64756C655F4C6F674D67742C6D6F64756C655F44656C6976657279576179734D67742C6D6F64756C655F44656C69766572794D65726368616E744D67742C6D6F64756C655F706179496E746572666163652C6D6F64756C655F44656C697665727952756C65734C6973742C6D6F64756C655F696D6167652C6D6F64756C655F73656E736974697665576F72642C6D6F64756C655F52656379636C65496E666F2C6D6F64756C655F646F6D61696E2C6D6F64756C655F5061676553454F2C6D6F64756C655F6170694170702C6D6F64756C655F53696D706C65496E666F2C6D6F64756C655F4E6F726D616C53696D706C65496E666F2C6D6F64756C655F696D706F7274436F6C756D6E2C6D6F64756C655F6E6F746963652C6D6F64756C655F737973617267756D656E745F6F70656E2C6D6F64756C655F6170704C6F67696E222C2264656661756C74526F6F74223A22636F6C5F53657474696E67526F6F74227D";

        String ts = hexToString(t);

        System.out.println(ts);

        byte[] buf = new byte[1024 * 128];

        FileInputStream fis = new FileInputStream(new File("c:\\a.xml"));

        int len = fis.read(buf);

        StringBuilder sb = new StringBuilder();

        String xml = new String(buf, "utf8");

        int pos = 0;

        while (true) {
            int b = xml.indexOf("<class>", pos);
            if (b < 0) {
                break;
            }
            b += 7;
            int e = xml.indexOf("</class>", b);
            if (e < 0) {
                break;
            }
            String ClassName = xml.substring(b, e);
            String t1 = xml.substring(pos, b);
            sb.append(t1);
            sb.append("net.xinshi.pigeon.saas.cron.CronAgent");
            int x = xml.indexOf("<parameters>", e);
            if (x < 0) {
                break;
            }
            x += 12;
            int y = xml.indexOf("</parameters>", e);
            if (y < 0) {
                break;
            }
            String t2 = xml.substring(e, x);
            sb.append(t2);
            sb.append(ClassName);
            if (x < y) {
                sb.append(" ");
            }
            pos = x;
        }
        sb.append(xml.substring(pos));


        int hash = DefaultHashGenerator.hash("p_51827_164673");
        int y = hash;
        System.out.print("处理进度:");
        for (int i = 0; i < 100; i++) {
            String per = "" + i;
            int n = 5 - per.length();
            for (int f = 0; f < n; f++) {
                per += " ";
            }
            if (i > 0) {
                System.out.print("\b\b\b\b\b");
            }
            System.out.print(per);
        }
    }

}

