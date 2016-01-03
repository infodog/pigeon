package net.xinshi.pigeon.test;

import com.sun.imageio.plugins.jpeg.JPEGImageWriter;
import net.xinshi.pigeon.distributed.util.DefaultHashGenerator;
import net.xinshi.pigeon.server.distributedserver.fileserver.InfoZipFile;
import net.xinshi.pigeon.util.LRUMap;
import org.imgscalr.Scalr;
import sun.security.pkcs.PKCS7;
import sun.security.pkcs.SignerInfo;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.plugins.jpeg.JPEGImageWriteParam;
import javax.imageio.stream.ImageOutputStream;
import java.awt.image.BufferedImage;
import java.io.*;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-4-10
 * Time: 下午2:58
 * To change this template use File | Settings | File Templates.
 */

public class UnZip {

    private static boolean isVerified(byte[] sig, byte[] content) {
        PKCS7 pkcs7;
        X509Certificate[] x509s;
        X509Certificate x509;
        SignerInfo[] ss;
        SignerInfo s;
        Signature sign;
        try {
            pkcs7 = new PKCS7(sig);
            x509s = pkcs7.getCertificates();
            x509 = x509s[0];


            System.out.println(pkcs7.getContentInfo().getContent().getAsString());
            ss = pkcs7.getSignerInfos();
            s = ss[0];

            // sign = Signature.getInstance("SHA1/RSA", "BC");

            sign = Signature.getInstance("SHA1WithRSA");

            sign.initVerify(x509);


            //sign.update(toUnicode(content));toUnicode(content)

            byte[] aa = s.getEncryptedDigest();
            System.out.println(new String(aa));
            boolean verified = sign.verify(s.getEncryptedDigest());

            ///////////////////////////////////////////////////////
            pkcs7 = null;
            sign = null;
            s = null;
            ss = null;
            x509 = null;
            x509s = null;
            return verified;
        } catch (SignatureException sigex) {
            //System.out.println("VerifyP7sTool.isVerified22222222");
            sigex.printStackTrace();
            // System.out.println("sigexcept " + sigex.toString());
            return false;
        } catch (Exception secex) {
            //System.out.println("VerifyP7sTool.isVerified3333333333");
            secex.printStackTrace();
            // System.out.println("other exception " + secex.toString());
            return false;
        }
    }

    public static java.security.cert.Certificate getCertFromString(String certString) throws UnsupportedEncodingException, IOException,
            CertificateException, java.security.cert.CertificateException {
        java.security.cert.Certificate cert = null;
        byte[] buf = certString.getBytes("GBK");
        BufferedInputStream bis = new BufferedInputStream(new ByteArrayInputStream(buf));
        while (bis.available() > 0) {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            cert = cf.generateCertificate(bis);
        }
        return cert;
    }

/*    public static byte[] getLicenseData(String LicenseFile, java.security.cert.Certificate cert) throws
            FileNotFoundException, IOException, SignatureException,
            InvalidKeyException, NoSuchAlgorithmException, IOException,
            CertificateException, FileNotFoundException, Exception {
        Map m = parseLicenseFile(LicenseFile);
        byte[] data = (byte[]) m.get("data");
        byte[] digest = (byte[]) m.get("digest");
        if (!verify(cert, data, digest)) {
            throw new Exception("License is not valid");
        } else {
            return data;
        }
    }*/


    public static void main(String argv[]) {
        try {

            String[] s = ":".split(":");

            int hix = DefaultHashGenerator.hash("module_ShopTemplate");

            long r = Long.parseLong("");

            // TestImages.test("60X60", "c:\\aa.jpg", "c:\\aa_100X100.jpg");

           /* boolean rv = CheckLicense.check();

            {
                String Cert = "-----BEGIN CERTIFICATE-----\r\n";
                Cert += "MIICtDCCAh0CAXMwDQYJKoZIhvcNAQEFBQAwgZoxCzAJBgNVBAYTAkNOMRIwEAYD\r\n";
                Cert += "VQQIEwlHdWFuZ0RvbmcxEjAQBgNVBAcTCUd1YW5nWmhvdTESMBAGA1UEChMJSW5m\r\n";
                Cert += "b3NjYXBlMRQwEgYDVQQLEwtIZWFkIE9mZmljZTEcMBoGA1UEAxMTSW5mb0NBJ3Mg\r\n";
                Cert += "VGVzdCBSb290MTEbMBkGCSqGSIb3DQEJARYMenh5QDIxY24uY29tMB4XDTAwMDYw\r\n";
                Cert += "NDE4MDM1MVoXDTAwMDkxMjE4MDM1MVowgakxCzAJBgNVBAYTAkNOMRIwEAYDVQQI\r\n";
                Cert += "EwlHdWFuZ2RvbmcxEjAQBgNVBAcTCUd1YW5nemhvdTESMBAGA1UEChMJSW5mb3Nj\r\n";
                Cert += "YXBlMRQwEgYDVQQLEwtEZXZlbG9wbWVudDEdMBsGA1UEAxMUSW5mb3NjYXBlIFRl\r\n";
                Cert += "Y2hub2xvZ3kxKTAnBgkqhkiG9w0BCQEWGmluZm9tYXJrdEBpbmZvc2NhcGUuY29t\r\n";
                Cert += "LmNuMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDXMIzFZT/TpV/igugePI9t\r\n";
                Cert += "saNt6cYYwUfOZEGV2ACRGGCGdkxQUVqYWPZXBqBUxSVRMenvl8J4jUa+JXcsHdkv\r\n";
                Cert += "R4fXws7SVM+0iqHeDrhbixN4wZoBukS9RRJ6Llzi0r1DYSjn9d+OXVrfCqz2pEgF\r\n";
                Cert += "0MTRe5eYNE1jZgbFA1b8PQIDAQABMA0GCSqGSIb3DQEBBQUAA4GBAIn4QsdxDcs+\r\n";
                Cert += "11DF3HD6l/G9R8hoAe5hQORis5s9xERBp6NFJHTqC9Mii3I6EAaO3F0P0OriSBV0\r\n";
                Cert += "LyBwAQT+455egEAS9eiIsnZ9Op65TCdfuVd04ZNvTE/1HP+OhWkiXiGcxpy0vbY6\r\n";
                Cert += "oU1YsAF6YPwSFPeKuQwXjm5qaTPscu6G\r\n";
                Cert += "-----END CERTIFICATE-----\r\n";

                java.security.cert.Certificate cert = getCertFromString(Cert);

                int i = 0;

            }

            {
                File f0 = new File("c:\\key");

                FileInputStream fis0 = new FileInputStream(f0);
                byte[] buf0 = new byte[10240];
                int n0 = fis0.read(buf0);
                byte[] out0 = Base64.decodeBase64(buf0);
                ++n0;

                File f = new File("c:\\fjtlicense.lic");
                FileInputStream fis = new FileInputStream(f);
                byte[] buf = new byte[10240];
                int n = fis.read(buf);
                byte[] out = Base64.decodeBase64(buf);
                ++n;

                isVerified(out, out0);


            }*/

            {
                String filePath = "c:\\aa.jpg";
                String spec = "60X60";
                String extPart = "jpg";


                File fo = new File(filePath);

                FileInputStream fis = new FileInputStream(fo);

                byte [] xr = new byte[(int)fo.length()];

                fis.read(xr);

                long t0 = System.currentTimeMillis();

                 byte[] xb =  JAISampleProgram.resizeImageAsJPG(xr, 60);  // TestImages.resizeImageAsJPG(xr, 60);

                long t1 = System.currentTimeMillis();

                System.out.println("resize = " + (t1 - t0) + " ms");

                if (xb != null) {
                    FileOutputStream fos = new FileOutputStream("c:\\c.jpg");
                    fos.write(xb);
                    fos.close();
                }

                BufferedImage img = ImageIO.read(fis);
                fis.close();
                String[] xy = spec.split("X");
                int x = Integer.parseInt(xy[0]);
                int y = Integer.parseInt(xy[1]);
                BufferedImage resizedImage = Scalr.resize(img, x, y, null);
                ByteArrayOutputStream bs = new ByteArrayOutputStream();
                if (extPart.equalsIgnoreCase("jpg") || extPart.equalsIgnoreCase("jpeg")) {
                    try {
                        JPEGImageWriter imageWriter = (JPEGImageWriter) ImageIO.getImageWritersBySuffix("jpeg").next();
                        ImageOutputStream ios = ImageIO.createImageOutputStream(bs);
                        imageWriter.setOutput(ios);
                        JPEGImageWriteParam jpegParams = (JPEGImageWriteParam) imageWriter.getDefaultWriteParam();
                        jpegParams.setCompressionMode(JPEGImageWriteParam.MODE_EXPLICIT);
                        jpegParams.setCompressionQuality(0.95f);
                        IIOMetadata data = imageWriter.getDefaultImageMetadata(new ImageTypeSpecifier(resizedImage), jpegParams);
                        imageWriter.write(data, new IIOImage(resizedImage, null, null), jpegParams);
                        bs.close();
                        imageWriter.dispose();
                    } catch (Exception e) {
                        e.printStackTrace();
                        return;
                    }
                } else {
                    ImageOutputStream imOut = ImageIO.createImageOutputStream(bs);
                    ImageIO.write(resizedImage, extPart, imOut);
                }
                FileOutputStream fos = new FileOutputStream("c:\\b.jpg");
                fos.write(bs.toByteArray());
                fos.close();
            }


            final int MAX_FLUSH_DB_QUEUE = 100000;
            final int ALARM_DB_QUEUE = (int) (MAX_FLUSH_DB_QUEUE * 0.999f);

            byte[] sBuf = new byte[0];

            String nil = new String(sBuf, "UTF-8");

            int hi = DefaultHashGenerator.hash("workspace_product");

            String[] a = "aaa_bbb_@ccc_dd".split("x");

            SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");
            String today = format.format(new Date());
            System.out.println(today);


            String[] ps = ",1,,2,".split(",");

            Map test = new LRUMap(50);

            for (int i = 0; i < 1000; i++) {
                test.put(i, i);
            }
            Integer e = (Integer) test.get(951);
            Integer o = (Integer) test.values().toArray()[0];

            InfoZipFile.decompressFile("c:\\wptpass.zip", "c:\\1234");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

