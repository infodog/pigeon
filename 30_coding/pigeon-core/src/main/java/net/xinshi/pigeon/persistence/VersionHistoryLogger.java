package net.xinshi.pigeon.persistence;

import net.xinshi.pigeon.distributed.duplicate.DuplicateService;
import net.xinshi.pigeon.util.CommonTools;
import net.xinshi.pigeon.util.TimeTools;
import org.apache.commons.lang.StringUtils;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import javax.sql.DataSource;
import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-2-13
 * Time: 下午6:12
 * To change this template use File | Settings | File Templates.
 */

public class VersionHistoryLogger {

    public static final int TailMagicNumber = 0x03ABCDEF;
    public static final byte[] bytesMagic = int2bytes(TailMagicNumber);
    public static final int RotateNumber = 1000000;

    private long Version = -1L;
    private Object verMutex = new Object();
    private String LoggerDirectory = null;
    private FileOutputStream logfos = null;
    private long filesno = -1;
    private long filecount = 0;
    private DataSource ds = null;
    private String versionTableName;
    private String versionKeyName;
    private long dbVersion = -1L;
    private long lastRotate = 0L;

    VersionPosition versionPosition = new VersionPosition(100, 1000);

    public long getDbVersion() {
        synchronized (verMutex) {
            return dbVersion;
        }
    }

    public long getLastRotate() {
        return lastRotate;
    }

    public void setVersionKeyName(String versionKeyName) {
        this.versionKeyName = versionKeyName;
    }

    public void setVersionTableName(String versionTableName) {
        this.versionTableName = versionTableName;
    }

    public void setDs(DataSource ds) {
        this.ds = ds;
    }

    private void setVersion(long version) {
        synchronized (verMutex) {
            if (version != Version && version != Version + 1) {
                System.out.println("panic! VersionHistoryLogger setVersion() old = " + Version + ", new = " + version);
                try {
                    throw new Exception("panic! VersionHistoryLogger setVersion() old = " + Version + ", new = " + version);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            Version = version;
        }
    }

    private long newVersion() {
        synchronized (verMutex) {
            if (Version < 0) {
                return Version;
            }
            return ++Version;
        }
    }

    private long getFilesnoByVersion(long version) {
        return version / RotateNumber;
    }

    private String getTheLoggerFile(long version) {
        if (LoggerDirectory != null) {
            return LoggerDirectory + "/" + getFilesnoByVersion(version) + ".bin";
        }
        return null;
    }

    class SortFileName implements Comparator<String> {
        public int compare(String s1, String s2) {
            int i1 = 0;
            int i2 = 0;
            try {
                i1 = Integer.valueOf(s1.split("\\.")[0]);
                i2 = Integer.valueOf(s2.split("\\.")[0]);
                return i1 - i2;
            } catch (Exception e) {
                e.printStackTrace();
            }
            return 0;
        }
    }

    public List<String> getAllLoggerFile() {
        if (LoggerDirectory == null) {
            return null;
        }
        try {
            File f = new File(LoggerDirectory);
            f = new File(f.getAbsolutePath());
            if (!f.exists()) {
                f.mkdirs();
            }
            if (!f.isDirectory()) {
                return null;
            }
            String[] fileNames = f.list();
            filecount = fileNames.length;
            if (fileNames == null || fileNames.length == 0) {
                return null;
            }
            ArrayList<String> listNames = new ArrayList();
            for (String name : fileNames) {
                String[] parts = name.split("\\.");
                int no = Integer.valueOf(parts[0]).intValue();
                if (no < 0 || parts[1].compareToIgnoreCase("bin") != 0) {
                    throw new Exception("bad bin logger file : " + name);
                }
                listNames.add(name);
            }
            Collections.sort(listNames, new SortFileName());
            return listNames;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private String getLastLoggerFile() {
        try {
            List<String> names = getAllLoggerFile();
            if (names == null || names.size() < 1) {
                return null;
            }
            return names.get(names.size() - 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private void listAllVersion() {
        try {
            List<String> names = getAllLoggerFile();
            if (names != null) {
                for (String name : names) {
                    long[] minmax = getMinMaxVersion(this.getLoggerDirectory() + "/" + name);
                    if (minmax != null && minmax.length == 2) {
                        System.out.println(name + " : [" + minmax[0] + ", " + minmax[1] + "]");
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private synchronized long getLastVersion() {
        try {
            String filename = getLastLoggerFile();
            synchronized (verMutex) {
                if (filename == null) {
                    Version = 0L;
                } else {
                    Version = getMaxVersion(this.getLoggerDirectory() + "/" + filename);
                }
                return Version;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    private boolean appendFileA2FileB(FileInputStream fis, FileOutputStream fos) {
        try {
            byte[] buffer = new byte[8192];
            while (true) {
                int count = fis.read(buffer);
                if (count > 0) {
                    fos.write(buffer, 0, count);
                    fos.flush();
                }
                if (count != 8192) {
                    break;
                }
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    void getVersionNumberFromDB() throws Exception {
        boolean isOK = false;
        DataSourceTransactionManager txManager = new DataSourceTransactionManager();
        txManager.setDataSource(ds);
        PlatformTransactionManager transactionManager = txManager;
        DefaultTransactionDefinition dtf = new DefaultTransactionDefinition();
        dtf.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        TransactionStatus ts = transactionManager.getTransaction(dtf);
        String sql = String.format("select version from %s where name = ?", versionTableName);
        Connection conn = null;
        try {
            conn = ds.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setString(1, versionKeyName);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                dbVersion = rs.getLong(1);
            } else {
                rs.close();
                stmt.close();
                sql = String.format("insert into %s (name, version) values(?, ?)", versionTableName);
                stmt = conn.prepareStatement(sql);
                stmt.setString(1, versionKeyName);
                if (Version > 0) {
                    dbVersion = Version;
                } else {
                    dbVersion = 0;
                }
                stmt.setLong(2, dbVersion);
                stmt.execute();
                stmt.close();
            }
            isOK = true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null && conn.isClosed() == false) {
                conn.close();
            }
            if (isOK) {
                transactionManager.commit(ts);
            } else {
                transactionManager.rollback(ts);
                dbVersion = -1L;
            }
        }
    }

    public static boolean putVersionHistoryToFOS(VersionHistory vh, FileOutputStream fos) {
        try {
            byte[] size = int2bytes(vh.getBehindLength());
            byte[] version = long2bytes(vh.getVersion());
            fos.write(size);
            fos.write(vh.getData());
            fos.write(size);
            fos.write(version);
            fos.write(bytesMagic);
            fos.flush();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public static VersionHistory getVersionHistoryFromFIS(FileInputStream fis) {
        VersionHistory vh = null;
        try {
            byte[] bytes4 = new byte[4];
            byte[] bytes8 = new byte[8];
            int count = fis.read(bytes4);
            if (count < 1) {
                return null;
            }
            if (count != 4) {
                throw new Exception("getVersionHistoryFromFIS (count != 4) ...... ");
            }
            int len1 = bytes2int(bytes4);
            if (len1 < 0) {
                throw new Exception("getVersionHistoryFromFIS (len1 < 0) ...... ");
            }
            byte[] data = new byte[len1];
            count = fis.read(data);
            if (count != len1) {
                throw new Exception("getVersionHistoryFromFIS (count != len1) ...... ");
            }
            count = fis.read(bytes4);
            if (count != 4) {
                throw new Exception("getVersionHistoryFromFIS (count != 4) ...... ");
            }
            int len2 = bytes2int(bytes4);
            if (len1 != len2) {
                throw new Exception("getVersionHistoryFromFIS (len1 != len2) ...... ");
            }
            count = fis.read(bytes8);
            if (count != 8) {
                throw new Exception("getVersionHistoryFromFIS (count != 8) ...... ");
            }
            long version = bytes2long(bytes8);
            count = fis.read(bytes4);
            if (count != 4) {
                throw new Exception("getVersionHistoryFromFIS (count != 4) ...... ");
            }
            int magic = bytes2int(bytes4);
            if (magic != TailMagicNumber) {
                throw new Exception("getVersionHistoryFromFIS (magic != TailMagicNumber) ...... ");
            }
            vh = new VersionHistory(len1, data, len2, version, magic);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return vh;
    }

    public InputStream getInputStreamFromVersionHistoryFile(FileInputStream fis) throws Exception {
        synchronized (fis) {
            InputStream mis = null;
            VersionHistory vh = getVersionHistoryFromFIS(fis);
            if (vh != null) {
                mis = new ByteArrayInputStream(vh.getData());
            }
            return mis;
        }
    }

    public byte[] getBytesFromVersionHistoryFile(FileInputStream fis) throws Exception {
        synchronized (fis) {
            VersionHistory vh = getVersionHistoryFromFIS(fis);
            if (vh != null) {
                return vh.getData();
            }
            return null;
        }
    }

    public long[] getMinMaxVersion(String filename) throws Exception {
        long min = 0;
        long max = 0;
        RandomAccessFile rf = null;
        try {
            File f = new File(filename);
            if (!f.exists()) {
                System.out.println("getMinMaxVersion file not exists : " + filename);
                return null;
            }
            rf = new RandomAccessFile(f, "r");
            long len = rf.length();
            if (len == 0) {
                System.out.println("getMinMaxVersion file length == 0 : " + filename);
                return null;
            }
            if (len < 20) {
                throw new Exception("isAtomFile (len < 20) ...... ");
            }
            {
                int size = rf.readInt();
                if (size < 0) {
                    throw new Exception("isAtomFile (size < 0) ...... ");
                }
                if (size + 20 > len) {
                    throw new Exception("isAtomFile (size + 16 > len) ...... ");
                }
                rf.seek(size + 4);
                int size2 = rf.readInt();
                if (size != size2) {
                    throw new Exception("isAtomFile (size != size2) ...... ");
                }
                long version = rf.readLong();
                int magic = rf.readInt();
                if (magic != this.TailMagicNumber) {
                    throw new Exception("isAtomFile (magic != this.TailMagicNumber) ...... ");
                }
                min = version;
            }
            {
                rf.seek(len - 16);
                int size2 = rf.readInt();
                if (size2 < 0) {
                    throw new Exception("isAtomFile (size2 < 0) ...... ");
                }
                if (size2 + 20 > len) {
                    throw new Exception("isAtomFile (size2 + 20 > len) ...... ");
                }
                long version = rf.readLong();
                int magic = rf.readInt();
                if (magic != this.TailMagicNumber) {
                    throw new Exception("isAtomFile (magic != this.TailMagicNumber) ...... ");
                }
                rf.seek(len - size2 - 20);
                int size = rf.readInt();
                if (size != size2) {
                    throw new Exception("isAtomFile (size != size2)) ...... ");
                }
                max = version;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rf != null) {
                rf.close();
            }
        }
        if (min == 0 || max == 0) {
            throw new Exception("isAtomFile (min == 0 || max == 0) ...... ");
        }
        return new long[]{min, max};
    }

    private long getMinVersion(String filename) throws Exception {
        RandomAccessFile rf = null;
        try {
            rf = new RandomAccessFile(new File(filename), "r");
            long len = rf.length();
            if (len < 20) {
                return -2;
            }
            int size = rf.readInt();
            if (size < 0) {
                return -3;
            }
            if (size + 20 > len) {
                return -4;
            }
            rf.seek(size + 4);
            int size2 = rf.readInt();
            if (size != size2) {
                return -5;
            }
            long version = rf.readLong();
            int magic = rf.readInt();
            if (magic == this.TailMagicNumber) {
                return version;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rf != null) {
                rf.close();
            }
        }
        return -1;
    }

    private long getMaxVersion(String filename) throws Exception {
        RandomAccessFile rf = null;
        try {
            rf = new RandomAccessFile(new File(filename), "r");
            long len = rf.length();
            if (len == 0) {
                return 0;
            }
            if (len < 20) {
                return -2;
            }
            rf.seek(len - 16);
            int size2 = rf.readInt();
            if (size2 + 20 > len) {
                return -3;
            }
            long version = rf.readLong();
            int magic = rf.readInt();
            if (magic != this.TailMagicNumber) {
                return -4;
            }
            rf.seek(len - size2 - 20);
            int size = rf.readInt();
            if (size != size2) {
                return -5;
            }
            return version;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rf != null) {
                rf.close();
            }
        }
        return -1;
    }

    private synchronized long appendData(long version, byte[] data) {
        if (version > 0 && data.length > 0) {
            try {
                synchronized (verMutex) {
                    long fsno = getFilesnoByVersion(version);
                    if (fsno != filesno) {
                        if (logfos != null) {
                            logfos.close();
                            logfos = null;
                        }
                        filesno = fsno;
                    }
                    if (logfos == null) {
                        logfos = new FileOutputStream(getTheLoggerFile(version), true);
                        if (logfos == null) {
                            return -1;
                        }
                    }
                    byte[] lens = int2bytes(data.length);
                    logfos.write(lens);
                    logfos.write(data);
                    logfos.write(lens);
                    logfos.write(long2bytes(version));
                    logfos.write(bytesMagic);
                    logfos.flush();
                }
                return version;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return -1;
    }

    public synchronized long writeData(long version, byte[] data) {
        if (version > 0 && data.length > 0) {
            try {
                synchronized (verMutex) {
                    long fsno = getFilesnoByVersion(version);
                    if (fsno != filesno) {
                        if (logfos != null) {
                            logfos.close();
                            logfos = null;
                        }
                        filesno = fsno;
                    }
                    if (logfos == null) {
                        logfos = new FileOutputStream(getTheLoggerFile(version), true);
                        if (logfos == null) {
                            return -1;
                        }
                    }
                    byte[] lens = int2bytes(data.length);
                    logfos.write(lens);
                    logfos.write(data);
                    logfos.write(lens);
                    logfos.write(long2bytes(version));
                    logfos.write(bytesMagic);
                    logfos.flush();
                    setVersion(version);
                }
                return version;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return -1;
    }

    public static byte[] int2bytes(int v) {
        byte[] writeBuffer = new byte[4];
        writeBuffer[0] = (byte) (v >>> 24);
        writeBuffer[1] = (byte) (v >>> 16);
        writeBuffer[2] = (byte) (v >>> 8);
        writeBuffer[3] = (byte) v;
        return writeBuffer;
    }

    public static int bytes2int(byte[] writeBuffer) {
        int v = 0;
        v |= (writeBuffer[0] & 0xFF) << 24;
        v |= (writeBuffer[1] & 0xFF) << 16;
        v |= (writeBuffer[2] & 0xFF) << 8;
        v |= (writeBuffer[3] & 0xFF);
        return v;
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

    private static long bytes2long(byte[] writeBuffer) {
        long v = 0;
        v |= (long) (writeBuffer[0] & 0xFF) << 56;
        v |= (long) (writeBuffer[1] & 0xFF) << 48;
        v |= (long) (writeBuffer[2] & 0xFF) << 40;
        v |= (long) (writeBuffer[3] & 0xFF) << 32;
        v |= (long) (writeBuffer[4] & 0xFF) << 24;
        v |= (long) (writeBuffer[5] & 0xFF) << 16;
        v |= (long) (writeBuffer[6] & 0xFF) << 8;
        v |= (long) (writeBuffer[7] & 0xFF);
        return v;
    }

    public long logVersionAndData(long version, byte[] data, FileOutputStream logfos) {
        if (version > 0 && data.length > 0) {
            try {
                synchronized (verMutex) {
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
                    setVersion(version);
                }
                return version;

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return -1;
    }

    public long getVersion() {
        synchronized (verMutex) {
            return Version;
        }
    }

    public long getVersionDistance(long ver) {
        synchronized (verMutex) {
            return ver - Version;
        }
    }

    public String getLoggerDirectory() {
        return LoggerDirectory;
    }

    public void setLoggerDirectory(String loggerDirectory) throws Exception {
        loggerDirectory = loggerDirectory.replace("\\", "/");
        loggerDirectory = loggerDirectory.replace("/./", "/");
        File file = new File(loggerDirectory);
        LoggerDirectory = file.getCanonicalPath();
    }

    public synchronized boolean init() {
        long lv = getLastVersion();
        System.out.println("VersionHistoryLogger(\"" + getLoggerDirectory() +
                "\") init : filecount = " + filecount + ", version : " + Version);
        if (filecount > 0 && lv == 0) {
            return false;
        }
        listAllVersion();
        try {
            getVersionNumberFromDB();
            if (dbVersion < 0) {
                return false;
            }
            System.out.println("dbVersion = " + dbVersion);
            if (dbVersion > Version) {
                System.out.println("warning!!! (dbVersion > Version) ......");
                synchronized (verMutex) {
                    Version = dbVersion;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return lv >= 0;
    }

    public synchronized void reloadVersion() {
        getLastVersion();
        try {
            getVersionNumberFromDB();
            if (dbVersion < 0) {
                return;
            }
            if (dbVersion > Version) {
                synchronized (verMutex) {
                    Version = dbVersion;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public long logVersionHistory(byte[] data) {
        synchronized (verMutex) {
            long ver = newVersion();
            if (ver > 0) {
                ver = writeData(ver, data);
            }
            return ver;
        }
    }

    public long logVersionHistory(byte[] data, FileOutputStream logfos) {
        synchronized (verMutex) {
            synchronized (logfos) {
                long ver = newVersion();
                if (ver > 0) {
                    ver = logVersionAndData(ver, data, logfos);
                }
                return ver;
            }
        }
    }

    public synchronized boolean rotateVersionHistory(String logfile) throws Exception {
        FileInputStream fis = null;
        try {
            long[] minmax = getMinMaxVersion(logfile);
            if (minmax == null) {
                return true;
            }
            lastRotate = System.currentTimeMillis();
            if (minmax.length == 2) {
                fis = new FileInputStream(new File(logfile));
                if (getFilesnoByVersion(minmax[0]) == getFilesnoByVersion(minmax[1])) {
                    synchronized (verMutex) {
                        long fsno = getFilesnoByVersion(minmax[0]);
                        if (fsno != filesno) {
                            if (logfos != null) {
                                logfos.close();
                                logfos = null;
                            }
                            filesno = fsno;
                        }
                        if (logfos == null) {
                            logfos = new FileOutputStream(getTheLoggerFile(minmax[0]), true);
                            if (logfos == null) {
                                return false;
                            }
                        }
                        if (minmax[1] > Version) {
                            setVersion(minmax[1]);
                        }
                    }
                    return appendFileA2FileB(fis, logfos);
                } else {
                    while (true) {
                        VersionHistory vh = getVersionHistoryFromFIS(fis);
                        if (vh == null) {
                            break;
                        }
                        synchronized (verMutex) {
                            if (vh.getVersion() > Version) {
                                setVersion(vh.getVersion());
                            }
                            if (appendData(vh.getVersion(), vh.getData()) != vh.getVersion()) {
                                return false;
                            }
                        }
                    }
                }
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (fis != null) {
                fis.close();
            }
        }
        return false;
    }

    public synchronized boolean rotateVersionHistory(Object obj, String logfile) throws Exception {
        try {
            long max = 0L;
            long[] minmax = getMinMaxVersion(logfile);
            if (minmax.length == 2) {
                max = minmax[1];
            }
            boolean rb = rotateVersionHistory(logfile);
            if (rb) {
                DuplicateService.updateListDataItems(obj, max);
            }
            return rb;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public synchronized VersionHistory[] rangeVersionHistory(long min, long max) throws Exception {
        if (max < min) {
            throw new Exception("rangeVersionHistory (max < min) ...... ");
        }
        if (min < 1) {
            min = 1;
        }
        if (max > getVersion()) {
            max = getVersion();
        }
        int count = (int) (max - min) + 1;
        if (count < 1) {
            throw new Exception("rangeVersionHistory (count < 1) ...... ");
        }
        VersionHistory[] vhs = new VersionHistory[count];
        int index = 0;
        long cur = min;
        long ver = min;
        FileInputStream fis = null;
        while (true) {
            try {
                if (fis != null) {
                    fis.close();
                    fis = null;
                }
                String filename = getTheLoggerFile(cur);
                File f = new File(filename);
                if (!f.exists()) {
                    //System.out.println("rangeVersionHistory " + filename + " not exists ... cur = " + cur);
                    return  new VersionHistory[0];
//                    cur = (cur + RotateNumber) / RotateNumber * RotateNumber;
//                    if (cur > max) {
//                        break;
//                    }
//                    continue;
                }
                fis = new FileInputStream(f);
                VersionHistory tvh = null;
                long position = versionPosition.hits(filename, min);
                if (position > 0) {
                    fis.skip(position);
                }
                while (true) {
                    VersionHistory vh = getVersionHistoryFromFIS(fis);
                    if (vh != null) {
                        tvh = vh;
                        if (vh.getVersion() < min) {
                            continue;
                        }
                        if (vh.getVersion() >= max) {
                            long nv = vh.getVersion() + 1;
                            long np = fis.getChannel().position();
                            versionPosition.push(filename, nv, np);
                        }
                        if (vh.getVersion() > max) {
                            break;
                        }
                        if (index >= vhs.length) {
                            if (fis != null) {
                                fis.close();
                                fis = null;
                            }
                            throw new Exception("rangeVersionHistory (index >= vhs.length) ...... ");
                        }
                        if (vh.getVersion() >= ver) {
                            vhs[index++] = vh;
                            ver = vh.getVersion();
                        } else {
                            System.out.println("rangeVersionHistory (vh.getVersion() < ver) ...... " + vh.getVersion());
                        }
                    } else {
                        break;
                    }
                }
                if (tvh != null && tvh.getVersion() >= max) {
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (cur < max) {
                cur += RotateNumber;
                continue;
            }
            break;
        }
        if (fis != null) {
            fis.close();
            fis = null;
        }
        return vhs;
    }

    public synchronized ByteArrayOutputStream rangeVersionHistory(Object obj, long min, long max) throws Exception {
        int dn = 0;
        int mn = 0;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        VersionHistory[] vhs = rangeVersionHistory(min, max);
        long cur = min;
        if (vhs != null) {
            for (VersionHistory vh : vhs) {
                if (vh != null) {
                    cur = vh.getVersion();
                    CommonTools.writeLong(baos, vh.getVersion());
                    CommonTools.writeBytes(baos, vh.getData(), 0, vh.getData().length);
                    ++dn;
                }
            }
        }
        if (cur <= max) {
            mn = DuplicateService.appendDataItems(obj, cur, max, baos);
        }
        //System.out.println(TimeTools.getNowTimeString() + " rangeVersionHistory min = " + min + ", max = " + max + ", DiskNum = " + dn + ", MemNum = " + mn);
        return baos;
    }

    public static void main(String[] args) throws Exception {
        try {

            VersionHistoryLogger vhl = new VersionHistoryLogger();

            vhl.setLoggerDirectory("d:/share/");

            boolean bi = vhl.init();

            boolean br = vhl.rotateVersionHistory("c:/123.bin");

            for (long l = 1L; l <= vhl.getVersion(); l += 1000L) {
                VersionHistory[] vhs = vhl.rangeVersionHistory(l, l + 999L);
                if (vhs != null && vhs[0] != null) {
                    System.out.println(vhs[0].getVersion());
                }
            }

            FileOutputStream fos = null;

            String filename = "c:/123.bin";

            fos = new FileOutputStream(new File(filename));

            String s = StringUtils.repeat(".", 1024);

            for (int i = 0; i < 1000; i++) {
                long ver = vhl.logVersionHistory(s.getBytes(), fos);
                if (ver < 0) {
                    System.out.println("ver < 0");
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

