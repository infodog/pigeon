package net.xinshi.pigeon.tools.check;

import net.xinshi.pigeon.flexobject.FlexObjectEntry;
import net.xinshi.pigeon.persistence.VersionHistory;
import net.xinshi.pigeon.persistence.VersionHistoryLogger;
import net.xinshi.pigeon.util.CommonTools;

import java.io.*;
import java.util.ArrayList;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-6-18
 * Time: 下午4:19
 * To change this template use File | Settings | File Templates.
 */

public class PigeonCheckData {

    public static final int TailMagicNumber = 0x03ABCDEF;
    public static final byte[] bytesMagic = int2bytes(TailMagicNumber);

    public static long doit(String dir, String filename, String name, long ver, FileOutputStream osnb) throws Exception {
        FileInputStream is = new FileInputStream(filename);
        FileOutputStream os = new FileOutputStream(filename + ".checked");
        FileOutputStream osn = new FileOutputStream(dir + "/" + name + ".txt", true);
        long pre = -1;
        try {
            while (true) {
                VersionHistory vh = VersionHistoryLogger.getVersionHistoryFromFIS(is);
                if (vh == null) {
                    break;
                }
                if (pre != -1 && pre + 1 != vh.getVersion()) {
                    System.out.println("version wrong ... now = " + pre + ", next = " + vh.getVersion());
                }
                pre = vh.getVersion();
                //// System.out.println(vh.getVersion());
                // os.write(("" + vh.getVersion() + "\t").getBytes());
                // os.write(vh.getData());
                // os.write("\r\n".getBytes());
                if (name != null) {
                    {   // flexobject
                        InputStream mis = new ByteArrayInputStream(vh.getData());
                        FlexObjectEntry entry = CommonTools.readEntry(mis);
                        if (entry == null) {
                            System.out.println("bad entry is null version = " + pre);
                        } else {
                            if (entry.getContent().length() == 0 && entry.getBytesContent().length == 0) {
                                if (entry.getName().startsWith("c_")) {
                                    System.out.println("delete object [" + entry.getName() + "] version = " + pre);
                                }
                            }
                            /* if (entry.getName().equals(name)) {
                                if (entry.getContent().length() == 0 && entry.getBytesContent().length == 0) {
                                    System.out.println("delete object " + name + " version = " + pre);
                                } else {
                                    System.out.println("set object " + entry.getContent() + " version = " + pre);
                                }
                            }*/
                        }
                    }
                    /*String val = new String(vh.getData(), "utf8");
                    if (val.indexOf(name) >= 0) {
                        System.out.println(val);
                        osn.write(vh.getData());
                        osn.write("\r\n".getBytes());
                        if (val.length() == val.getBytes().length) {
                            logVersionAndData(ver++, vh.getData(), osnb);
                        }
                    }*/
                }
                VersionHistoryLogger.putVersionHistoryToFOS(vh, os);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("now version = " + pre);
        is.close();
        os.close();
        return ver;
    }


    public static byte[] int2bytes(int v) {
        byte[] writeBuffer = new byte[4];
        writeBuffer[0] = (byte) (v >>> 24);
        writeBuffer[1] = (byte) (v >>> 16);
        writeBuffer[2] = (byte) (v >>> 8);
        writeBuffer[3] = (byte) v;
        return writeBuffer;
    }


    private static byte[] long2bytes(long v) {
        byte[] writeBuffer = new byte[8];
        writeBuffer[0] = (byte) (v >>> 56);
        writeBuffer[1] = (byte) (v >>> 48);
        writeBuffer[2] = (byte) (v >>> 40);
        writeBuffer[3] = (byte) (v >>> 32);
        writeBuffer[4] = (byte) (v >>> 24);
        writeBuffer[5] = (byte) (v >>> 16);
        writeBuffer[6] = (byte) (v >>> 8);
        writeBuffer[7] = (byte) v;
        return writeBuffer;
    }

    public static long logVersionAndData(long version, byte[] data, FileOutputStream logfos) {
        if (version > 0 && data.length > 0) {
            try {

                if (logfos == null) {
                    return -1;
                }
                byte[] lens = int2bytes(data.length);
                logfos.write(lens);
                logfos.write(data);
                logfos.write(lens);
                logfos.write(long2bytes(version));
                logfos.write(bytesMagic);
                logfos.flush();
                return version;

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return -1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("args is empty!");
            return;
        }
        ArrayList<String> files = new ArrayList<String>();
        String dir = "";
        File fdir = new File(args[0]);
        if (fdir.isFile()) {
            files.add(fdir.getAbsolutePath());
            dir = fdir.getParent();
        } else {
            for (File f : fdir.listFiles()) {
                files.add(f.getAbsolutePath());
            }
            dir = fdir.getAbsolutePath();
        }
        String name = null;
        if (args.length > 1) {
            name = args[1];
        }
        long ver = 1;
        FileOutputStream osnb = new FileOutputStream(dir + "/" + name + ".bin", true);
        for (String f : files) {
            ver = doit(dir, f, name, ver, osnb);
        }
    }
}
