package net.xinshi.pigeon.saas.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-8-23
 * Time: 下午4:50
 * To change this template use File | Settings | File Templates.
 */

public class BuildCronXml {

    public static void main(String[] args) throws Exception {

        System.out.println("BuildCronXml");

        if (args.length < 1) {
            throw new Exception("args.length < 1");
        }
        File f = new File(args[0]);
        if (f.length() < 1) {
            throw new Exception("f.length() < 1");
        }

        byte[] buf = new byte[(int) f.length()];

        FileInputStream fis = new FileInputStream(f);

        int len = fis.read(buf);

        StringBuilder sb = new StringBuilder();

        String xml = new String(buf, "utf8");

        boolean gbk = false;
        if (xml.indexOf("encoding=\"GBK\"") > 0 || xml.indexOf("encoding=\"gbk\"") > 0) {
            gbk = true;
            System.out.println("This file is GBK encoding");
            xml = new String(buf, "gbk");
        } else {
            System.out.println("This file is UTF-8 encoding");
        }

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
            sb.append("net.xinshi.saasadmin.cron.CronAgent");
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
        fis.close();

        FileOutputStream fos = new FileOutputStream(f);
        byte[] bytes = null;
        if (gbk) {
            bytes = sb.toString().getBytes("GBK");
        } else {
            bytes = sb.toString().getBytes("utf8");
        }
        fos.write(bytes);
        fos.close();

    }

}
