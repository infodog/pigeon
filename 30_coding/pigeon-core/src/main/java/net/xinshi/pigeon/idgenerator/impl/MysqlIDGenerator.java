package net.xinshi.pigeon.idgenerator.impl;

import net.xinshi.pigeon.distributed.bean.DataItem;
import net.xinshi.pigeon.distributed.duplicate.DuplicateService;
import net.xinshi.pigeon.idgenerator.IIDGenerator;
import net.xinshi.pigeon.persistence.IPigeonPersistence;
import net.xinshi.pigeon.persistence.PersistenceService;
import net.xinshi.pigeon.persistence.VersionHistoryLogger;
import net.xinshi.pigeon.status.Constants;
import net.xinshi.pigeon.util.CommonTools;
import net.xinshi.pigeon.util.SoftHashMap;
import net.xinshi.pigeon.util.TimeTools;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static net.xinshi.pigeon.status.Constants.getStateString;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-27
 * Time: 上午9:30
 * To change this template use File | Settings | File Templates.
 */

public class MysqlIDGenerator implements IIDGenerator, IPigeonPersistence {
    private DataSource ds;
    public String tableName = "t_ids";
    String versionTableName = "t_pigeontransaction";
    String versionKeyName;
    public VersionHistoryLogger verLogger;
    int state_word = net.xinshi.pigeon.status.Constants.NORMAL_STATE;
    String logDirectory;
    long savedbfailedcount = 0;
    protected String lock_sql = "Lock tables t_ids write";
    protected String unlock_sql = "Unlock tables";

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
        mapStatus.put("cache_string", "");
        return mapStatus;
    }

    public DataSource getDs() {
        return ds;
    }

    public void setDs(DataSource ds) {
        this.ds = ds;
    }

    public void setVersionKeyName(String versionKeyName) {
        this.versionKeyName = versionKeyName;
    }

    public void setLogDirectory(String logDirectory) {
        this.logDirectory = logDirectory;
    }

    class IdPair {
        public IdPair() {
            curVal = 0;
            maxVal = 0;
        }

        public int curVal;
        public int maxVal;
    }

    ConcurrentHashMap Ids = null;

    public synchronized void set_state_word(int state_word) throws Exception {
        this.state_word = state_word;
    }

    private void updateLastVersion(String Table, String Name, long version) throws Exception {
        boolean isOK = false;
        Connection _conn = null;
        PreparedStatement stmt = null;
        DataSource ds = this.ds;
        _conn = ds.getConnection();
        try {
            stmt = _conn.prepareStatement("Update " + Table + " set version = ? where name= '" + Name + "'");
            stmt.setLong(1, version);
            stmt.execute();
            stmt.close();
            isOK = true;
            savedbfailedcount = 0;
        } catch (Exception e) {
            e.printStackTrace();
            savedbfailedcount++;
            throw e;
        } finally {
            if (isOK) {
                _conn.commit();
            } else {
                _conn.rollback();
            }
            if (_conn != null && _conn.isClosed() == false) {
                _conn.close();
            }
        }
    }

    synchronized public long getId(String Name) throws Exception {
        int idForThisTime;
        if (Ids == null) {
            Ids = new SoftHashMap();
        }
        IdPair id = (IdPair) Ids.get(Name);
        if (id == null) {
            id = new IdPair();
            Ids.put(Name, id);
        }
        if (id.curVal < id.maxVal) {
            idForThisTime = id.curVal++;
            return idForThisTime;
        }
        if (state_word != Constants.NORMAL_STATE) {
            throw new Exception("pigeon is READONLY ...... ");
        }
        boolean isOK = false;
        Connection _conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        DataSource ds = this.ds;
        _conn = ds.getConnection();
        try {
            _conn.setAutoCommit(false);
            stmt = _conn.prepareStatement(lock_sql);
            stmt.execute();
            stmt.close();
            stmt = _conn.prepareStatement("select count(*) as c from t_ids where TableName=? ");
            stmt.setString(1, Name);
            rs = stmt.executeQuery();
            if (rs.next()) {
                int c = rs.getInt(1);
                rs.close();
                stmt.close();
                if (c == 0) {
                    stmt = _conn.prepareStatement("Insert into t_ids(TableName,NextValue)values(?,?)");
                    stmt.setString(1, Name);
                    stmt.setInt(2, 50000);
                    stmt.execute();
                    stmt.close();
                }
            } else {
                rs.close();
                stmt.close();
            }
            stmt = _conn.prepareStatement("select NextValue from t_ids where TableName=?",
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
            stmt.setString(1, Name);
            rs = stmt.executeQuery();
            rs.next();
            int ID = rs.getInt("NextValue");
            rs.close();
            stmt.close();
            stmt = _conn.prepareStatement("Update t_ids set NextValue=NextValue+100 where TableName=?");
            stmt.setString(1, Name);
            stmt.execute();
            stmt.close();
            stmt = _conn.prepareStatement(unlock_sql);
            stmt.execute();
            stmt.close();
            isOK = true;
            id.curVal = ID;
            id.maxVal = ID + 100;
            idForThisTime = id.curVal++;
            savedbfailedcount = 0;
        } catch (Exception e) {
            e.printStackTrace();
            savedbfailedcount++;
            throw e;
        } finally {
            if (isOK) {
                _conn.commit();
            } else {
                _conn.rollback();
            }
            if (_conn != null && _conn.isClosed() == false) {
                _conn.close();
            }
        }
        return idForThisTime;
    }

    public long setSkipValue(String name, long value) throws Exception {
        throw new Exception("method setSkipValue(...) forbidden!");
    }

    public synchronized void writeVersionLogAndCache(long version, String s) throws Exception {
        if (!Constants.canWriteLog(state_word)) {
            throw new Exception("pigeon is READONLY ...... ");
        }
        byte[] data = s.getBytes("UTF-8");
        String[] parts = s.split(":");
        String name = parts[0];
        long id = Long.valueOf(parts[1]);
        long delta = verLogger.getVersionDistance(version);
        if (delta < 1) {
            System.out.println("writeVersionLogAndCache delta = " + delta);
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
        updateIdByName(name, (int) id);
        long ver = verLogger.writeData(version, data);
        if (ver < 1) {
            throw new Exception("verLogger.writeData failed");
        }
        updateLastVersion(versionTableName, versionKeyName, ver);
    }

    public ByteArrayOutputStream pullDataItems(long min, long max) throws Exception {
        return verLogger.rangeVersionHistory(this, min, max);
    }

    public synchronized void writeVersionLogAndCacheRaw(long version, String s) throws Exception {
        if (!Constants.canWriteLog(state_word)) {
            throw new Exception("pigeon is READONLY ...... ");
        }
        long delta = verLogger.getVersionDistance(version);
        if (delta < 1) {
            System.out.println("writeVersionLogAndCacheRaw delta = " + delta + ", version = " + version);
            return;
        } else if (delta > 1) {
            throw new Exception("critic! writeLogAndCacheRaw delta = " + delta + ", version = " + version);
        }
        byte[] data = s.getBytes("UTF-8");
        String[] parts = s.split(":");
        String name = parts[0];
        long id = Long.valueOf(parts[1]);
        updateIdByName(name, (int) id);
        long ver = verLogger.writeData(version, data);
        if (ver < 1) {
            throw new Exception("verLogger.writeData failed");
        }
        updateLastVersion(versionTableName, versionKeyName, ver);
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
        System.out.println(TimeTools.getNowTimeString() + " idserver syncVersion begin = " + begin + ", end = " + end + ", verNum = " + verNum + ", syncNum = " + syncNum);
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
            } catch (Exception e) {
                e.printStackTrace();
                verInit = false;
            }
            if (!verInit) {
                System.out.println("atom VersionHistoryLogger init failed");
                set_state_word(Constants.READONLY_STATE);
                System.exit(-1);
            }
            verLogger.reloadVersion();
            PersistenceService.syncVersionTask(this);
        }
    }

    public synchronized long updateIdByName(String Name, int id) throws Exception {
        Connection _conn = null;
        boolean isOK = false;
        if (state_word != Constants.NORMAL_STATE) {
            throw new Exception("pigeon is READONLY ...... ");
        }
        PreparedStatement stmt = null;
        ResultSet rs = null;
        DataSource ds = this.ds;
        _conn = ds.getConnection();
        try {
            _conn.setAutoCommit(false);
            stmt = _conn.prepareStatement("select count(*) as c from t_ids where TableName=? ");
            stmt.setString(1, Name);
            rs = stmt.executeQuery();
            if (rs.next()) {
                int c = rs.getInt(1);
                rs.close();
                stmt.close();
                if (c == 0) {
                    stmt = _conn.prepareStatement("Insert into t_ids(TableName,NextValue)values(?,?)");
                    stmt.setString(1, Name);
                    stmt.setInt(2, 50000);
                    stmt.execute();
                    stmt.close();
                }
            } else {
                rs.close();
                stmt.close();
            }
            stmt = _conn.prepareStatement("Update t_ids set NextValue=" + id + " where TableName=?");
            stmt.setString(1, Name);
            stmt.execute();
            stmt.close();
            isOK = true;
            savedbfailedcount = 0;
            return id;
        } catch (Exception e) {
            e.printStackTrace();
            savedbfailedcount++;
            throw e;
        } finally {
            if (isOK) {
                _conn.commit();
            } else {
                _conn.rollback();
            }
            if (_conn != null && _conn.isClosed() == false) {
                _conn.close();
            }
        }
    }

    public synchronized long getIdAndForwardOrig(String Name, int forwardNum) throws Exception {
        Connection _conn = null;
        long ID;
        boolean isOK = false;

        if (state_word != Constants.NORMAL_STATE) {
            throw new Exception("pigeon is READONLY ...... ");
        }
        PreparedStatement stmt = null;
        ResultSet rs = null;
        DataSource ds = this.ds;
        _conn = ds.getConnection();
        try {
            _conn.setAutoCommit(false);
            stmt = _conn.prepareStatement(lock_sql);
            stmt.execute();
            stmt.close();
            stmt = _conn.prepareStatement("select count(*) as c from t_ids where TableName=? ");
            stmt.setString(1, Name);
            rs = stmt.executeQuery();
            if (rs.next()) {
                int c = rs.getInt(1);
                rs.close();
                stmt.close();
                if (c == 0) {
                    stmt = _conn.prepareStatement("Insert into t_ids(TableName,NextValue)values(?,?)");
                    stmt.setString(1, Name);
                    stmt.setInt(2, 50000);
                    stmt.execute();
                    stmt.close();
                }
            } else {
                rs.close();
                stmt.close();
            }
            stmt = _conn.prepareStatement("select NextValue from t_ids where TableName=?",
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
            stmt.setString(1, Name);
            rs = stmt.executeQuery();
            rs.next();
            ID = rs.getInt("NextValue");
            rs.close();
            stmt.close();
            {
                String line = Name + ":" + (ID + forwardNum);
                byte[] data = line.getBytes("UTF-8");
                long ver = verLogger.logVersionHistory(data);
                if (ver < 1) {
                    throw new Exception("logVersionHistory failed");
                }
            }
            stmt = _conn.prepareStatement("Update t_ids set NextValue=NextValue + " + forwardNum + " where TableName=?");
            stmt.setString(1, Name);
            stmt.execute();
            stmt.close();
            isOK = true;
            savedbfailedcount = 0;
            return ID;
        } catch (Exception e) {
            e.printStackTrace();
            savedbfailedcount++;
            throw e;
        } finally {
            try {
                stmt = _conn.prepareStatement(unlock_sql);
                stmt.execute();
                stmt.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (isOK) {
                _conn.commit();
            } else {
                _conn.rollback();
            }
            if (_conn != null && _conn.isClosed() == false) {
                _conn.close();
            }
        }
    }

    public long getIdAndForward(String Name, int forwardNum) throws Exception {
        long id = 0L;
        DataItem di = null;
        if (DuplicateService.syncQueueOverflow(this)) {
            throw new Exception("id server DuplicateService.syncQueueOverflow() ... check sync copy or flush db ... ");
        }
        synchronized (this) {
            id = getIdAndForwardOrig(Name, forwardNum);
            long ver = verLogger.getVersion();
            if (ver < 1) {
                throw new Exception("verLogger.getVersion() failed");
            }
            updateLastVersion(versionTableName, versionKeyName, ver);
            if (DuplicateService.isMaster(this)) {
                String line = Name + ":" + (id + forwardNum);
                byte[] data = line.getBytes("UTF-8");
                di = DuplicateService.duplicateData(this, ver, data);
                DuplicateService.updateListDataItems(this, ver);
            }
        }
        if (di != null && !di.waitme(1000 * 30)) {
            throw new Exception("id server getIdAndForward DuplicateService.duplicateData() failed ...... ");
        }
        return id;
    }

}

