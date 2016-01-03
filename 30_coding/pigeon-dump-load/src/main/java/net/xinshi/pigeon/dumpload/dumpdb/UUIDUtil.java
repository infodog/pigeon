package net.xinshi.pigeon.dumpload.dumpdb;

import org.apache.commons.codec.binary.Base64;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-5-10
 * Time: 上午10:19
 * To change this template use File | Settings | File Templates.
 */
public class UUIDUtil {

    // 将 s 进行 BASE64 编码

    public static String getBASE64(byte[] bytes) {
        return (new String(Base64.encodeBase64(bytes)));
    }

    public static String getBASE64(String s) {
        if (s == null) return null;
        // return (new sun.misc.BASE64Encoder()).encode( s.getBytes() );
        return (new String(Base64.encodeBase64(s.getBytes())));
    }

    // 将 BASE64 编码的字符串 s 进行解码

    public static String getFromBASE64(String s) {
        if (s == null) return null;
        // BASE64Decoder decoder = new BASE64Decoder();
        try {
            byte[] b = Base64.decodeBase64(s.getBytes());
            return new String(b);
        } catch (Exception e) {
            return null;
        }
    }

    public static String bufferToHex(byte bytes[]) {
        return bufferToHex(bytes, 0, bytes.length);
    }

    private static String bufferToHex(byte bytes[], int m, int n) {
        StringBuffer stringbuffer = new StringBuffer(2 * n);
        int k = m + n;
        for (int l = m; l < k; l++) {
            appendHexPair(bytes[l], stringbuffer);
        }
        return stringbuffer.toString();
    }

    private static char hexDigits[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    private static void appendHexPair(byte bt, StringBuffer stringbuffer) {
        char c0 = hexDigits[(bt & 0xf0) >> 4];
        char c1 = hexDigits[bt & 0xf];
        stringbuffer.append(c0);
        stringbuffer.append(c1);
    }

    public static String getMD5Str(String str) throws Exception {
        String s = "";

        MessageDigest messagedigest = null;
        try {
            messagedigest = MessageDigest.getInstance("MD5");
            messagedigest.update(str.getBytes());
            s = bufferToHex(messagedigest.digest());
        } catch (NoSuchAlgorithmException nsaex) {
            System.err.println("MessageDigest?????MD5");
            nsaex.printStackTrace();
        }

        return s;
    }

    private static long md5byteToLong(byte[] b) {
        if (b.length != 16) {
            return 0;
        }

        long s = 0;
        long s0 = (b[0] & 0xff) ^ (b[15] - '0');
        long s1 = (b[1] & 0xff) ^ (b[14] - '0');
        long s2 = (b[2] & 0xff) ^ (b[13] - '0');
        long s3 = (b[3] & 0xff) ^ (b[12] - '0');
        long s4 = (b[4] & 0xff) ^ (b[11] - '0');
        long s5 = (b[5] & 0xff) ^ (b[10] - '0');
        long s6 = (b[6] & 0xff) ^ (b[9] - '0');
        long s7 = (b[7] & 0xff) ^ (b[8] - '0');

        s1 <<= 8;
        s2 <<= 16;
        s3 <<= 24;
        s4 <<= 8 * 4;
        s5 <<= 8 * 5;
        s6 <<= 8 * 6;
        s7 <<= 8 * 7 - 1;
        s = s0 | s1 | s2 | s3 | s4 | s5 | s6 | s7;
        s %= 2147483647;

        return s;
    }

    public static int getMD5int(String str) throws Exception {
        // long i = 0;

        return getMD5Str(str).hashCode();

        /*
        MessageDigest messagedigest = null;
        try {
            messagedigest = MessageDigest.getInstance("MD5");
            messagedigest.update(str.getBytes());
            byte[] b = messagedigest.digest();
            i = md5byteToLong(b);
        } catch (NoSuchAlgorithmException nsaex) {
            System.err.println("MessageDigest?????MD5");
            nsaex.printStackTrace();
        }

        return i;
        */
    }

    public static String getUUIDStr() {
        UUID uuid = UUID.randomUUID();
        return uuid.toString();
    }
}



