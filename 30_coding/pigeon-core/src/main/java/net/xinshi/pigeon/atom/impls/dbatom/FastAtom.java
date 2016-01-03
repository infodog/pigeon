package net.xinshi.pigeon.atom.impls.dbatom;

import net.xinshi.pigeon.atom.IIntegerAtom;
import net.xinshi.pigeon.cache.CacheLogger;
import net.xinshi.pigeon.distributed.bean.DataItem;
import net.xinshi.pigeon.distributed.bean.ServerConfig;
import net.xinshi.pigeon.distributed.duplicate.DuplicateService;
import net.xinshi.pigeon.persistence.IPigeonPersistence;
import net.xinshi.pigeon.persistence.PersistenceService;
import net.xinshi.pigeon.persistence.VersionHistoryLogger;
import net.xinshi.pigeon.status.Constants;
import net.xinshi.pigeon.util.CommonTools;
import net.xinshi.pigeon.util.TimeTools;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

import static net.xinshi.pigeon.status.Constants.getStateString;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-27
 * Time: 上午10:08
 * To change this template use File | Settings | File Templates.
 */

public class FastAtom implements IIntegerAtom, IPigeonPersistence {
    DataSource ds;
    String tableName;
    String versionTableName = "t_pigeontransaction";
    String versionKeyName;
    String logDirectory;
    PlatformTransactionManager txManager;
    boolean dbOK = true;
    boolean stateChange = true;
    Object stateMonitor;
    //Object flusherWaiter;
    boolean flusherStopped = true;
    int state_word = net.xinshi.pigeon.status.Constants.NORMAL_STATE;
    public VersionHistoryLogger verLogger;
    int maxCacheEntries = 100000;
    CacheLogger cacheLogger;
    boolean fastCreate = false;
    long savedbfailedcount = 0;
    ServerConfig serverConfig = null;

    public ServerConfig getServerConfig() {
        return serverConfig;
    }

    public void setServerConfig(ServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

    private String getCacheString() {
        return cacheLogger.getCacheString();
    }

    public Map getStatusMap() {
        Map<String, String> mapStatus = new HashMap<String, String>();
        mapStatus.put("state_word", getStateString(state_word));
        mapStatus.put("version", String.valueOf(verLogger.getVersion()));
        if (verLogger.getLastRotate() > 0) {
            mapStatus.put("last_rotate", (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(verLogger.getLastRotate()));
        } else {
            mapStatus.put("last_rotate", "");
        }
        mapStatus.put("save_db_failed_count", String.valueOf(savedbfailedcount));
        mapStatus.put("cache_string", getCacheString());
        return mapStatus;
    }

    public int getMaxCacheEntries() {
        return maxCacheEntries;
    }

    public void setMaxCacheEntries(int maxCacheEntries) {
        this.maxCacheEntries = maxCacheEntries;
        // System.out.println("Atom maxCacheNumber = " + maxCacheEntries);
    }

    public boolean isFastCreate() {
        return fastCreate;
    }

    public void setFastCreate(boolean fastCreate) {
        this.fastCreate = fastCreate;
    }

    public DataSource getDs() {
        return ds;
    }

    public void setDs(DataSource ds) {
        this.ds = ds;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getVersionTableName() {
        return versionTableName;
    }

    public void setVersionTableName(String versionTableName) {
        this.versionTableName = versionTableName;
    }

    public String getVersionKeyName() {
        return versionKeyName;
    }

    public void setVersionKeyName(String versionKeyName) {
        this.versionKeyName = versionKeyName;
    }

    public PlatformTransactionManager getTxManager() {
        return txManager;
    }

    public void setTxManager(PlatformTransactionManager txManager) {
        this.txManager = txManager;
    }

    public String getLogDirectory() {
        return logDirectory;
    }

    public FastAtom() {
        //flusherWaiter = new Object();
        stateMonitor = new Object();
    }

    public void set_state_word(int state_word) throws Exception {
        this.state_word = state_word;
        if (state_word == Constants.NORMAL_STATE) {
            return;
        }
//        synchronized (flusherWaiter) {
//            flusherWaiter.notify();
//        }
        synchronized (stateMonitor) {
            stateChange = false;
        }
        if (flusherStopped) {
            return;
        }
        while (true) {
            synchronized (stateMonitor) {
                stateMonitor.wait(100);
                if (stateChange) {
                    break;
                }
            }
        }
    }

    public void setLogDirectory(String logDirectory) {
        if (!logDirectory.endsWith("/") && !logDirectory.endsWith("\\")) {
            logDirectory = logDirectory + "/";
        }
        this.logDirectory = logDirectory;
        File f = new File(logDirectory);
        f = new File(f.getAbsolutePath());
        this.logDirectory = f.getAbsolutePath();
        if (!f.exists()) {
            f.mkdirs();
        }
    }

    private String getOperationLogFileName() {
        if (!logDirectory.endsWith("/") && (!logDirectory.endsWith("\\"))) {
            logDirectory += "/";
        }
        return logDirectory + tableName + ".log";
    }

    private String getOldOperationLogFileName() {
        if (!logDirectory.endsWith("/") && (!logDirectory.endsWith("\\"))) {
            logDirectory += "/";
        }
        return logDirectory + tableName + ".oldlog";
    }

    public ByteArrayOutputStream pullDataItems(long min, long max) throws Exception {
        return verLogger.rangeVersionHistory(this, min, max);
    }

    @Override
    public long getVersion() {
        return verLogger.getVersion();
    }

    Object syncOnceLocker = new Object();

    public void syncVersion(long begin, long end) throws Exception {
        int verNum = 0;
        int syncNum = 0;
        synchronized (syncOnceLocker) {
            long min = begin;
            long max = min + 1000L;
            while (true) {
                if (max > end) {
                    max = end;
                }
                if (min > max) {
                    break;
                }
                InputStream is = DuplicateService.pullData(this, min, max);
                while (true) {
                    long v = 0L;
                    String line;
                    try {
                        v = CommonTools.readLong(is);
                        byte[] buf = CommonTools.readBytes(is);
                        line = new String(buf, "UTF-8");
                        ++verNum;
                    } catch (Exception e) {
                        break;
                    }
                    try {
                        writeVersionLogAndCacheRaw(v, line);
                        ++syncNum;
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw e;
                    }
                }
                min = max + 1;
                max += 1000L;
            }
        }
        System.out.println(TimeTools.getNowTimeString() + " atom syncVersion begin = " + begin + ", end = " + end + ", verNum = " + verNum + ", syncNum = " + syncNum);
    }

    public DataItem writeLogAndDuplicate(String s) throws Exception {
        if (!Constants.canWriteLog(state_word)) {
            throw new Exception("pigeon is READONLY ...... ");
        }
        if (DuplicateService.syncQueueOverflow(this)) {
            throw new Exception("atom DuplicateService.syncQueueOverflow() ... check sync copy or flush db ... ");
        }
        DataItem di = null;
        byte[] data = s.getBytes("UTF-8");
        synchronized (cacheLogger) {
            long ver = verLogger.logVersionHistory(data, cacheLogger.getLoggerFOS());
            if (ver < 1) {
                throw new Exception("logVersionHistory failed");
            }
            if (DuplicateService.isMaster(this)) {
                di = DuplicateService.duplicateData(this, ver, data);
            }
            cacheLogger.flush();
        }
        return di;
    }

    private DataItem writeLog(String opname, String atomName, long opValue) throws Exception {
        String line = opname + " " + atomName + " " + opValue + "\n";
        return writeLogAndDuplicate(line);
    }

    private DataItem writeLog(String opname, String atomName, long testValue, long incValue) throws Exception {
        String line = opname + " " + atomName + " " + testValue + " " + incValue + "\n";
        return writeLogAndDuplicate(line);
    }

    public void writeVersionLogAndCacheRaw(long version, String s) throws Exception {
        if (!Constants.canWriteLog(state_word)) {
            throw new Exception("pigeon is READONLY ...... ");
        }
        byte[] data = s.getBytes("UTF-8");
        synchronized (cacheLogger) {
            long delta = verLogger.getVersionDistance(version);
            if (delta < 1) {
                System.out.println("writeVersionLogAndCacheRaw delta = " + delta + ", version = " + version);
                return;
            } else if (delta > 1) {
                throw new Exception("critic! writeLogAndCacheRaw delta = " + delta + ", version = " + version);
            }
            long ver = verLogger.logVersionAndData(version, data, cacheLogger.getLoggerFOS());
            if (ver < 1) {
                throw new Exception("logVersionHistory failed");
            }
            cacheLogger.flush();
            addStringToCache(s);
        }
    }

    public void writeVersionLogAndCache(long version, String s) throws Exception {
        if (!Constants.canWriteLog(state_word)) {
            throw new Exception("pigeon is READONLY ...... ");
        }
        byte[] data = s.getBytes("UTF-8");
        synchronized (cacheLogger) {
            long delta = verLogger.getVersionDistance(version);
            if (delta < 1) {
                System.out.println("writeLogAndCache delta = " + delta);
                return;
            }
            if (delta > 1) {
                long min = version - delta + 1;
                long max = version - 1;
                syncVersion(min, max);
            }
            delta = verLogger.getVersionDistance(version);
            if (delta < 1) {
                System.out.println("after syncVersion delta = " + delta);
                return;
            }
            if (delta > 1) {
                throw new Exception("critic! after syncVersion delta = " + delta);
            }
            long ver = verLogger.logVersionAndData(version, data, cacheLogger.getLoggerFOS());
            if (ver < 1) {
                throw new Exception("logVersionHistory failed");
            }
            cacheLogger.flush();
            addStringToCache(s);
        }
    }

    private void deleteOldLog() {
        File f = new File(getOldOperationLogFileName());
        if (f.exists()) {
            try {
                if (!verLogger.rotateVersionHistory(this, f.getAbsolutePath())) {
                    System.out.println("atom rotateVersionHistory failed enter READONLY");
                    set_state_word(Constants.READONLY_STATE);
                    return;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            f.delete();
        }
    }

    private void deleteLog() {
        File f = new File(getOperationLogFileName());
        if (f.exists()) {
            f.delete();
        }
    }

    private boolean flushSnapShotToDB() throws SQLException {
        if (cacheLogger.noSavingDirtyCache()) {
            return true;
        }
        long lastVersion = -1L;
        PlatformTransactionManager transactionManager = this.txManager;
        DefaultTransactionDefinition dtf = new DefaultTransactionDefinition();
        dtf.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        TransactionStatus ts = transactionManager.getTransaction(dtf);
        Connection conn = null;
        boolean isok = false;
        try {
            conn = ds.getConnection();
            String updateSQL = String.format("update %s set value=? where name=? ", this.tableName);
            String insertSQL = String.format("insert into %s (name,value)values(?,?) ", this.tableName);
            PreparedStatement updateStmt = conn.prepareStatement(updateSQL);
            PreparedStatement insertStmt = conn.prepareStatement(insertSQL);
            for (Iterator it = cacheLogger.getSavingDirtyCache().entrySet().iterator(); it.hasNext(); ) {
                Map.Entry entry = (Map.Entry) it.next();
                Long l = new Long((Long) entry.getValue());
                String name = (String) entry.getKey();
                updateStmt.setLong(1, l);
                updateStmt.setString(2, name);
                updateStmt.execute();
                if (updateStmt.getUpdateCount() == 0) {
                    insertStmt.setString(1, name);
                    insertStmt.setLong(2, l);
                    insertStmt.execute();
                }
            }
            lastVersion = 0L;
            try {
                long[] minmax = verLogger.getMinMaxVersion(getOldOperationLogFileName());
                if (minmax != null) {
                    lastVersion = minmax[1];
                } else {
                    System.out.println("atom flush minmax == null ... (enter READONLY ) size : " + cacheLogger.getSavingDirtyCache().size());
                    set_state_word(Constants.READONLY_STATE);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (lastVersion > 0) {
                PreparedStatement versionUpdateStmt = conn.prepareStatement(String.format("update %s set version=? where name=? ", this.versionTableName));
                PreparedStatement versionInsertStmt = conn.prepareStatement(String.format("insert into %s (name,version)values(?,?)", this.versionTableName));
                versionUpdateStmt.setLong(1, lastVersion);
                versionUpdateStmt.setString(2, this.versionKeyName);
                versionUpdateStmt.execute();
                if (versionUpdateStmt.getUpdateCount() == 0) {
                    versionInsertStmt.setString(1, this.versionKeyName);
                    versionInsertStmt.setLong(2, lastVersion);
                    versionInsertStmt.execute();
                }
                transactionManager.commit(ts);
                isok = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (isok) {
                synchronized (cacheLogger) {
                    cacheLogger.swapSavingToCache();
                }
            } else {
                transactionManager.rollback(ts);
            }
            if (conn != null && conn.isClosed() == false) {
                conn.close();
            }
        }
        if (lastVersion < 1) {
            System.out.println("panic!!! atom (lastVersion < 1) = " + lastVersion);
        }
        dbOK = isok;
        return isok;
    }

    Long getNumberFromDB(String name) throws Exception {
        String sql = String.format("select value from %s where name=?", tableName);
        Connection conn = null;
        try {
            conn = ds.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, name);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getLong(1);
            } else {
                return null;
            }
        } finally {
            if (conn != null && conn.isClosed() == false) {
                conn.close();
            }
        }
    }

    private Long internalGet(String name) throws Exception {
        synchronized (cacheLogger) {
            Long v = (Long) cacheLogger.getFromCaches(name);
            if (v == null) {
                v = this.getNumberFromDB(name);
                if (v != null) {
                    cacheLogger.putToCache(name, v);
                }
            }
            return v;
        }
    }

    public void addStringToCache(String line) throws Exception {
        synchronized (cacheLogger) {
            line = line.trim();
            line.replace("\n", "");
            try {
                String[] parts = line.split(" ");
                String op = parts[0];
                String name = parts[1];
                if (op.equals("createAndSet")) {
                    String value = parts[2];
                    long lvalue = Long.parseLong(value);
                    Long v = this.internalGet(name);
                    if (!this.fastCreate && v != null) {
                        System.out.println("[ignore] createAndSet atom " + name + " old value = " + v);
                        return;
                    }
                    cacheLogger.putToDirtyCache(name, lvalue);
                } else if (op.equals("greaterAndInc")) {
                    Long v = this.internalGet(name);
                    String value = parts[2];
                    long testvalue = Long.parseLong(value);
                    long incvalue = Long.parseLong(parts[3]);
                    if (v == null) {
                        return;
                    }
                    if (v > testvalue) {
                        v += incvalue;
                    }
                    cacheLogger.putToDirtyCache(name, v);
                } else if (op.equals("lessAndInc")) {
                    Long v = this.internalGet(name);
                    String value = parts[2];
                    long testvalue = Long.parseLong(value);
                    long incvalue = Long.parseLong(parts[3]);
                    if (v == null) {
                        return;
                    }
                    if (v < testvalue) {
                        v += incvalue;
                    }
                    cacheLogger.putToDirtyCache(name, v);
                } else {
                    throw new Exception("not correct log format:" + line);
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        }
    }

    private void executeFile(File f) throws Exception {
        if (f.exists() == false) {
            return;
        }
        long[] minmax = verLogger.getMinMaxVersion(f.getAbsolutePath());
        if (minmax == null) {
            return;
        }
        if (verLogger.getDbVersion() == minmax[1]) {
            throw new Exception("getMinMaxVersion (verLogger.getDbVersion() == minmax[1]) : "
                    + f.getAbsolutePath());
        }
        if (verLogger.getDbVersion() >= minmax[0]) {
            throw new Exception("getMinMaxVersion (verLogger.getDbVersion() >= minmax[0]) : "
                    + f.getAbsolutePath());
        }
        FileInputStream fis = new FileInputStream(f);
        try {
            while (true) {
                byte[] bytes = null;
                try {
                    bytes = verLogger.getBytesFromVersionHistoryFile(fis);
                    if (bytes == null) {
                        break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("atom !!!!!!!!! replay file failed : " + f.getAbsolutePath());
                    set_state_word(Constants.READONLY_STATE);
                    System.exit(-1);
                }
                String line = new String(bytes, "UTF-8");
                addStringToCache(line);
            }
        } finally {
            if (fis != null) {
                fis.close();
            }
        }
    }

    private void replayOldLog() throws Exception {
        executeFile(new File(this.getOldOperationLogFileName()));
        cacheLogger.swapToSaving();
        System.out.println("DirtyCache size = " + cacheLogger.getDirtyCacheSize() + ", SavingDirtyCache size = " + cacheLogger.getSavingDirtyCacheSize());
        if (!this.flushSnapShotToDB()) {
            System.out.println("atom init flushSnapShotToDB failed");
            set_state_word(Constants.READONLY_STATE);
            System.exit(-1);
        }
        deleteOldLog();
    }

    private void replayLog() throws Exception {
        File fo = new File(this.getOldOperationLogFileName());
        if (fo.exists()) {
            throw new Exception("oldlog exists, can't replayLog()");
        }
        File fn = new File(this.getOperationLogFileName());
        if (fn.exists()) {
            if (fn.renameTo(fo)) {
                replayOldLog();
            } else {
                throw new Exception("rename failed, can't replayLog()");
            }
        }
    }

    boolean bInitialized = false;

    public void init() throws Exception {
        if (!bInitialized) {
            bInitialized = true;
            boolean verInit = false;
            try {
                verLogger = new VersionHistoryLogger();
                verLogger.setLoggerDirectory(logDirectory + "/share");
                verLogger.setDs(ds);
                verLogger.setVersionTableName("t_pigeontransaction");
                verLogger.setVersionKeyName(versionKeyName);
                verInit = verLogger.init();
                cacheLogger = new CacheLogger(maxCacheEntries, getOperationLogFileName(), getOldOperationLogFileName());
//                cacheLogger.setNotification(flusherWaiter);
            } catch (Exception e) {
                e.printStackTrace();
                verInit = false;
            }
            if (!verInit) {
                System.out.println("atom VersionHistoryLogger init failed");
                set_state_word(Constants.READONLY_STATE);
                System.exit(-1);
            }
            replayOldLog();
            while (!cacheLogger.noSavingDirtyCache()) {
                System.out.println("wait for flush to db ...... ");
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            replayLog();
            while (!cacheLogger.noSavingDirtyCache()) {
                System.out.println("wait for flush to db ...... ");
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            beginFlusher();
            verLogger.reloadVersion();
            PersistenceService.syncVersionTask(this);
        }
    }

    boolean noDirtyCache() {
        synchronized (cacheLogger) {
            if (state_word == Constants.NOWRITEDB_STATE && cacheLogger.noSavingDirtyCache()) {
                return true;
            }
            if (cacheLogger.noDirtyCache() && cacheLogger.noSavingDirtyCache()) {
                return true;
            }
            return false;
        }
    }

    void flush() {
        try {
            if (!dbOK) {
                System.out.println("atom dbOK == false, flush()");
            }
            int rc = -1;
            try {
                cacheLogger.swapToSavingAndRenameLog();
                rc = 0;
                if (flushSnapShotToDB()) {
                    deleteOldLog();
                    savedbfailedcount = 0;
                } else {
                    savedbfailedcount++;
                }
            } catch (Exception e) {
                if (rc == -1) {
                    e.printStackTrace();
                    System.out.println("atom swapToSavingAndRenameLog failed");
                    set_state_word(Constants.READONLY_STATE);
                }
                dbOK = false;
                savedbfailedcount++;
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    private void beginFlusher() {
        new Thread(new Flusher()).start();
    }

    class Flusher implements Runnable {
        public void run() {
            flusherStopped = false;
            Thread.currentThread().setName("FastAtom_Flusher_run");
            boolean waiting = true;
            long preTime = 0;
            String cacheString = "";
            while (true) {
                try {
                    if (preTime + 1000 * 600 < System.currentTimeMillis()) {
                        preTime = System.currentTimeMillis();
                        String cs = getCacheString();
                        if (cs.compareTo(cacheString) != 0) {
                            cacheString = cs;
                            System.out.println(TimeTools.getNowTimeString() + " Atom " + cs);
                        }
                    }
                    if (waiting) {
//                        synchronized (flusherWaiter) {
//                            flusherWaiter.wait(1000);
//                        }
                        try {
                            if (cacheLogger.getDirtyCacheSize() == 0) {
                                Thread.sleep(1000);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        synchronized (stateMonitor) {
                            if (stateChange && !Constants.canWriteDB(state_word)) {
                                continue;
                            }
                        }
                    }
                    flush();
                    synchronized (stateMonitor) {
                        if (!stateChange) {
                            if (noDirtyCache()) {
                                stateChange = true;
                                stateMonitor.notify();
                                if (Constants.isStop(state_word)) {
                                    break;
                                }
                            } else {
                                waiting = false;
                            }
                        } else {
                            waiting = true;
                        }
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
            flusherStopped = true;
        }
    }

    public void stop() throws InterruptedException {
    }

    public boolean createAndSet(String name, Integer initValue) throws Exception {
        DataItem di = null;
        synchronized (cacheLogger) {
            Long v = new Long(initValue);
            if (!this.fastCreate) {
                if (this.internalGet(name) != null) {
                    return false;
                }
            }
            di = writeLog("createAndSet", name, initValue);
            cacheLogger.putToDirtyCache(name, v);
        }
        if (di != null && !di.waitme(1000 * 5)) {
            System.out.println("atom createAndSet DuplicateService.duplicateData() failed ...... ");
        }
        return true;
    }

    @Override
    public boolean greaterAndInc(String name, int testValue, int incValue) throws Exception {
        long rl = greaterAndIncReturnLong(name, testValue, incValue);
        return true;
    }

    @Override
    public boolean lessAndInc(String name, int testValue, int incValue) throws Exception {
        long rl = lessAndIncReturnLong(name, testValue, incValue);
        return true;
    }

    public long greaterAndIncReturnLong(String name, int testValue, int incValue) throws Exception {
        Long v = 0L;
        DataItem di = null;
        synchronized (cacheLogger) {
            v = this.internalGet(name);
            if (v == null) {
                throw new Exception(name + " does not exists!");
            }
            if (v >= testValue) {
                v += incValue;
                di = writeLog("greaterAndInc", name, testValue, incValue);
                cacheLogger.putToDirtyCache(name, new Long(v));
            } else {
                throw new Exception("return false");
            }
        }
        if (di != null && !di.waitme(1000 * 5)) {
            System.out.println("atom greaterAndInc DuplicateService.duplicateData() failed ...... ");
        }
        return v;
    }

    public long lessAndIncReturnLong(String name, int testValue, int incValue) throws Exception {
        Long v = 0L;
        DataItem di = null;
        synchronized (cacheLogger) {
            v = this.internalGet(name);
            if (v == null) {
                throw new Exception(name + " does not exists!");
            }
            if (v <= testValue) {
                v += incValue;
                di = writeLog("lessAndInc", name, testValue, incValue);
                cacheLogger.putToDirtyCache(name, new Long(v));
            } else {
                throw new Exception("return false");
            }
        }
        if (di != null && !di.waitme(1000 * 5)) {
            System.out.println("atom lessAndInc DuplicateService.duplicateData() failed ...... ");
        }
        return v;
    }

    public Long get(String name) throws Exception {
        synchronized (cacheLogger) {
            return this.internalGet(name);
        }
    }

    public List<Long> getAtoms(List<String> atomIds) throws Exception {
        List<Long> result;
        result = new Vector<Long>();
        for (String id : atomIds) {
            result.add(internalGet(id));
        }
        return result;
    }

}

