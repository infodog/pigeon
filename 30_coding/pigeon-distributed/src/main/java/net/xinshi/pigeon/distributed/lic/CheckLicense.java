package net.xinshi.pigeon.distributed.lic;

import javax.security.cert.CertificateException;
import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.CertificateFactory;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: zxy
 * Date: 2009-12-12
 * Time: 22:16:05
 * To change this template use File | Settings | File Templates.
 */

public class CheckLicense {

    private boolean init = false;
    private String Cert = "-----BEGIN CERTIFICATE-----\r\n";
    private String licFileFinal = "emall.lic";
    private boolean print = false;

    public CheckLicense() {
        Cert += "MIIDCDCCAsWgAwIBAgIERpwtVjALBgcqhkjOOAQDBQAwZzELMAkGA1UEBhMCQ04xHjAcBgNVBAoT\r\n";
    }

    private void init() {
        if (!init) {
            String licFile = "@/";
            String Cert = this.Cert;
            Cert += "FUluZm9zY2FwZSBUZWNob25vbG9neTEnMCUGA1UECxMeSW5mb3NjYXBlIEVCdXNpbmVzcyBEZXBh\r\n";
            String licFile1 = "ic";
            Cert += "cnRtZW50MQ8wDQYDVQQDEwZqZW1hbGwwHhcNMDcwNzE3MDI0NTQyWhcNMTcxMjExMDI0NTQyWjBn\r\n";
            Cert += "MQswCQYDVQQGEwJDTjEeMBwGA1UEChMVSW5mb3NjYXBlIFRlY2hvbm9sb2d5MScwJQYDVQQLEx5J\r\n";
            licFileFinal = licFile + "p" + "i" + "g" + "eon" + ".l" + licFile1;
            Cert += "bmZvc2NhcGUgRUJ1c2luZXNzIERlcGFydG1lbnQxDzANBgNVBAMTBmplbWFsbDCCAbcwggEsBgcq\r\n";
            Cert += "hkjOOAQBMIIBHwKBgQD9f1OBHXUSKVLfSpwu7OTn9hG3UjzvRADDHj+AtlEmaUVdQCJR+1k9jVj6\r\n";
            Cert += "v8X1ujD2y5tVbNeBO4AdNG/yZmC3a5lQpaSfn+gEexAiwk+7qdf+t8Yb+DtX58aophUPBPuD9tPF\r\n";
            Cert += "HsMCNVQTWhaRMvZ1864rYdcq7/IiAxmd0UgBxwIVAJdgUI8VIwvMspK5gqLrhAvwWBz1AoGBAPfh\r\n";
            Cert += "oIXWmz3ey7yrXDa4V7l5lK+7+jrqgvlXTAs9B4JnUVlXjrrUWU/mcQcQgYC0SRZxI+hMKBYTt88J\r\n";
            Cert += "MozIpuE8FnqLVHyNKOCjrh4rs6Z1kW6jfwv6ITVi8ftiegEkO8yk8b6oUZCJqIPf4VrlnwaSi2Ze\r\n";
            Cert += "gHtVJWQBTDv+z0kqA4GEAAKBgAc3h8oH1Ps1yz3g850JN0lA/27Rg0g6SdQijwBvgXscSH4KVMba\r\n";
            Cert += "Da/6HMwjsPjdOWQFsPzz0fV3ZNLA9wH6aqaIYi2GHTD3JJfJ+4ZiHtw+u2zxbY9FFcpWetYzu6BR\r\n";
            Cert += "xLK7LOCl6FWDjTTHubLW6LNZ+Toct8BrZ8m5aBAlttjFMAsGByqGSM44BAMFAAMwADAtAhUAkPJg\r\n";
            Cert += "Z0a3LK+Ky6MzzITNA7aMUNoCFHaEFKfD+hBSkE4gzqWD1CLMARKX\r\n";
            Cert += "-----END CERTIFICATE-----";
            this.Cert = Cert;
            init = true;
        }
    }

    public boolean checkLicense() throws Exception, java.security.cert.CertificateException, CertificateException {
        init();
        String Cert = this.Cert;
        License lic = new License();
        java.security.cert.Certificate cert = License.CheckLicense.getCertFromString(Cert);
        return lic.isValid(licFileFinal, cert);
    }

    public static boolean check() throws Exception {
        CheckLicense cl = new CheckLicense();
        return cl.checkLicense();
    }

}

class License {
    private static boolean print = false;
    private java.util.Date beginDate;
    private java.util.Date endDate;
    Collection AllowedHosts;

    public License() {
        AllowedHosts = new Vector();
    }

    public java.util.Date getBeginDate() {
        return beginDate;
    }

    public void setBeginDate(java.util.Date beginDate) {
        this.beginDate = beginDate;
    }

    public java.util.Date getEndDate() {
        return endDate;
    }

    public void setEndDate(java.util.Date endDate) {
        this.endDate = endDate;
    }

    public Collection getAllowedHosts() {
        return AllowedHosts;
    }

    public java.util.Date formatDateStringToDateObject(String dateString) throws ParseException {
        String formatPattern = "yyyy-MM-dd HH:mm:ss";
        int length = dateString.length();
        java.util.Date date = null;
        SimpleDateFormat sdf = new SimpleDateFormat();
        sdf.applyPattern(formatPattern.substring(0, length));
        date = sdf.parse(dateString);
        return date;
    }

    public void parseLine(String line) throws ParseException {
        line = line.trim();
        if (line.startsWith("-")) {
            return;
        }
        String[] args = line.split("=");
        if (args.length != 2) {
            return;
        }
        if (args[0].equalsIgnoreCase("begindate")) {
            setBeginDate(formatDateStringToDateObject(args[1]));
        } else if (args[0].equalsIgnoreCase("enddate")) {
            setEndDate(formatDateStringToDateObject(args[1]));
        } else if (args[0].equalsIgnoreCase("AllowHost")) {
            AllowedHosts.add(args[1]);
        }
    }

    public void parseLicense(byte[] licenseData) throws IOException, ParseException {
        ByteArrayInputStream in = new ByteArrayInputStream(licenseData);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line = null;
        line = reader.readLine();
        while (line != null) {
            parseLine(line);
            line = reader.readLine();
        }
    }

    public boolean IsValid() throws UnknownHostException {
        Date now = new Date();
        if (getBeginDate() != null) {
            if (now.before(getBeginDate())) {
                return false;
            }
        }
        if (getEndDate() != null) {
            if (now.after(getEndDate())) {
                return false;
            }
        }
        if (getAllowedHosts().size() == 0) {
            return true;
        }
        InetAddress localAddr = InetAddress.getLocalHost();
        Iterator it = getAllowedHosts().iterator();
        while (it.hasNext()) {
            String hostAllowed = (String) it.next();
            if (hostAllowed.equalsIgnoreCase(localAddr.getHostAddress())) {
                return true;
            }
            if (hostAllowed.equalsIgnoreCase(localAddr.getHostName())) {
                return true;
            }
        }
        return false;
    }

    public boolean isValid(String LicenseFile, java.security.cert.Certificate cert) throws
            CertificateException, NoSuchAlgorithmException, InvalidKeyException,
            SignatureException, FileNotFoundException, IOException, Exception {
        byte[] licenseData = CheckLicense.getLicenseData(LicenseFile, cert);
        if (!License.print) {
            License.print = true;
            String str = new String(licenseData, "UTF-8");
            System.out.println("------------------------ begin license ------------------------");
            System.out.println(str);
            System.out.println("------------------------- end license -------------------------");
        }
        this.parseLicense(licenseData);
        return IsValid();
    }

    static class CheckLicense {
        public CheckLicense() {
        }

        public static void usage() {
            System.out.println("CheckLicense <certfile> <licensefile>");
        }

        public static Map parseLicenseFile(String LicenseFile) throws FileNotFoundException, IOException {
            InputStream fis = null;
            if (LicenseFile.startsWith("@")) {
                LicenseFile = LicenseFile.substring(1);
                fis = CheckLicense.class.getResourceAsStream(LicenseFile);
                if (fis == null) {
                    File directory = new File(".");
                    String licPath = directory.getAbsoluteFile() + "/../lib/pigeon.lic";
                    File file = new File(licPath);
                    fis = new FileInputStream(file);
                    if (!print) {
                        print = true;
                        System.out.println("license path : " + file.getCanonicalPath());
                    }
                }
            } else {
                fis = new FileInputStream(LicenseFile);
            }
            DataInputStream dis = new DataInputStream(fis);
            int plainDataLength = dis.readInt();
            byte[] plainData = new byte[plainDataLength];
            dis.read(plainData);
            int digestLength = dis.readInt();
            byte[] digest = new byte[digestLength];
            dis.read(digest);
            HashMap m = new HashMap();
            m.put("data", plainData);
            m.put("digest", digest);
            return m;
        }

        public static boolean verify(java.security.cert.Certificate cert, byte[] data,
                                     byte[] signature) throws FileNotFoundException, CertificateException, CertificateException,
                IOException, NoSuchAlgorithmException, InvalidKeyException,
                SignatureException {
            if (1 == 1) {
                Signature verify = Signature.getInstance("SHA1withDSA");
                verify.initVerify(cert);
                verify.update(data);
                return verify.verify(signature);
            } else {
                return true;
            }
        }

        public static byte[] getLicenseData(String LicenseFile, java.security.cert.Certificate cert) throws
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
    }

}
