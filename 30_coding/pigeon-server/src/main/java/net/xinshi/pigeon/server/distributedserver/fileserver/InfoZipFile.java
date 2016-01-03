package net.xinshi.pigeon.server.distributedserver.fileserver;

import org.apache.tools.zip.ZipEntry;
import org.apache.tools.zip.ZipFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-4-10
 * Time: 下午6:22
 * To change this template use File | Settings | File Templates.
 */

class StringTools {

    public static String getShortFileNameFromFilePath(String FilePath) {
        int position = FilePath.lastIndexOf("/");
        return FilePath.substring(position + 1);
    }

    public static String conversionSpecialCharacters(String convertedCharacters) {
        return convertedCharacters.replace("/", "\\");
    }

    public static String[] SegmentateCharacters(String str, String symbol) {
        return str.split(symbol);
    }

}

public class InfoZipFile {

    public static void decompressFile(String zipFilePath) {
        try {
            String tail = ".infozip";
            if (zipFilePath.length() < tail.length()) {
                return;
            }
            String ext = zipFilePath.substring(zipFilePath.length() - tail.length());
            if (ext.compareToIgnoreCase(".infozip") != 0) {
                return;
            }
            String dir = zipFilePath.substring(0, zipFilePath.length() - tail.length());
            decompressFile(zipFilePath, dir);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void decompressFile(String zipFilePath, String releasePath) throws IOException {
        ZipFile zipFile = null;
        try {
            zipFile = new ZipFile(zipFilePath, "GBK");
            Enumeration<ZipEntry> enumeration = zipFile.getEntries();
            InputStream inputStream = null;
            FileOutputStream fileOutputStream = null;
            ZipEntry zipEntry = null;
            String zipEntryNameStr = "";
            String[] zipEntryNameArray = null;
            if (!releasePath.endsWith("/") && !releasePath.endsWith("\\")) {
                releasePath += "/";
            }
            while (enumeration.hasMoreElements()) {
                zipEntry = enumeration.nextElement();
                zipEntryNameStr = zipEntry.getName();
                zipEntryNameArray = zipEntryNameStr.split("/");
                String path = releasePath;
                File root = new File(releasePath);
                if (!root.exists()) {
                    root.mkdir();
                }
                for (int i = 0; i < zipEntryNameArray.length; i++) {
                    if (i < zipEntryNameArray.length - 1) {
                        path = path + File.separator + zipEntryNameArray[i];
                        new File(StringTools.conversionSpecialCharacters(path)).mkdir();
                    } else {
                        if (StringTools.conversionSpecialCharacters(zipEntryNameStr).endsWith(File.separator)) {
                            new File(releasePath + zipEntryNameStr).mkdir();
                        } else {
                            inputStream = zipFile.getInputStream(zipEntry);
                            fileOutputStream = new FileOutputStream(new File(StringTools.conversionSpecialCharacters(releasePath + zipEntryNameStr)));
                            byte[] buf = new byte[4096];
                            int len;
                            while ((len = inputStream.read(buf)) > 0) {
                                fileOutputStream.write(buf, 0, len);
                            }
                            inputStream.close();
                            fileOutputStream.close();
                        }
                    }
                }
            }
        } finally {
            if (zipFile != null) {
                zipFile.close();
            }
        }
    }

}

