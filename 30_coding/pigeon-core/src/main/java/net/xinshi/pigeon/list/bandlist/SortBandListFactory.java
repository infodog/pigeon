package net.xinshi.pigeon.list.bandlist;

import net.xinshi.pigeon.distributed.bean.DataItem;
import net.xinshi.pigeon.distributed.bean.ServerConfig;
import net.xinshi.pigeon.distributed.duplicate.DuplicateService;
import net.xinshi.pigeon.list.IListFactory;
import net.xinshi.pigeon.list.ISortList;
import net.xinshi.pigeon.list.SortListObject;
import net.xinshi.pigeon.list.bandlist.bean.Band;
import net.xinshi.pigeon.persistence.IPigeonPersistence;
import net.xinshi.pigeon.persistence.PersistenceService;
import net.xinshi.pigeon.persistence.VersionHistoryLogger;
import net.xinshi.pigeon.status.Constants;
import net.xinshi.pigeon.util.CommonTools;
import net.xinshi.pigeon.util.CriticalSection;
import net.xinshi.pigeon.util.SoftHashMap;
import net.xinshi.pigeon.util.TimeTools;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import javax.sql.DataSource;
import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import static net.xinshi.pigeon.status.Constants.getStateString;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-27
 * Time: 下午3:18
 * To change this template use File | Settings | File Templates.
 */

public class SortBandListFactory implements IListFactory, IListBandService, IPigeonPersistence {
//    Logger logger = Logger.getLogger(Flusher.class.getName());
    private static Logger log = Logger.getLogger(SortBandListFactory.class.getName());
    IListBandDao dao;
    IBandSerializer bandSerializer;
    String factoryName;
    String logDirectory;
    ConcurrentHashMap dirtyBands;
    ConcurrentHashMap dirtyBandSnapShot;
    ConcurrentHashMap dirtyHeadBands;
    ConcurrentHashMap dirtyHeadBandsSnapShot;
    int limitOfDirtyBands = 5000;
    FileOutputStream logfos = null;
    PlatformTransactionManager txManager;
    PlatformTransactionManager idTxManager;
    DataSource idDataSource;
    private int maxListCacheSize = 100000;
    private int maxBandCacheSize = 100000;
    boolean stateChange = true;
    Object stateMonitor;
    //Object flusherWaiter;
    boolean flusherStopped = true;
    int state_word = net.xinshi.pigeon.status.Constants.NORMAL_STATE;
    public VersionHistoryLogger verLogger;
    Long maxBandID = -1L;
    DataSource ds;
    String versionTableName = "t_pigeontransaction";
    String versionKeyName;
    int savedbfailedcount = 0;
    Flusher flusher;
    Map bandCache = null;
    Map listCache = null;
    int maxObjectsPerBand = 500;
    int maxBandInfosPerBand = 500;
    Object globalLocker = new Object();
    CriticalSection listMutex = new CriticalSection(1000);
    boolean migration = false;
    ServerConfig serverConfig = null;

    public ServerConfig getServerConfig() {
        return serverConfig;
    }

    public void setServerConfig(ServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

    private String getCacheString() {
        return "listCache = " + listCache.size() + ", bandCache = " + bandCache.size() +
                ", dirtyBands = " + dirtyBands.size() + ", dirtyBandSnapShot = " + dirtyBandSnapShot.size() +
                ", dirtyHeadBands = " + dirtyHeadBands.size() + ", dirtyHeadBandsSnapShot = " + dirtyHeadBandsSnapShot.size();
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

    public void setDs(DataSource ds) {
        this.ds = ds;
    }

    public DataSource getDs() {
        return ds;
    }

    public void setVersionKeyName(String versionKeyName) {
        this.versionKeyName = versionKeyName;
    }

    public int getMaxListCacheSize() {
        return maxListCacheSize;
    }

    public void setMaxListCacheSize(int maxListCacheSize) {
        this.maxListCacheSize = maxListCacheSize;
        // System.out.println("list maxListCacheSize = " + maxListCacheSize);
    }

    public boolean isMigration() {
        return migration;
    }

    public void setMigration(boolean migration) {
        this.migration = migration;
    }

    public int getLimitOfDirtyBands() {
        return limitOfDirtyBands;
    }

    public boolean isFlushing() {
        return flusher.isFlushing();
    }

    public void flush() {
        flusher.flush();
    }

    public void setLimitOfDirtyBands(int limitOfDirtyBands) {
        this.limitOfDirtyBands = limitOfDirtyBands;
    }

    public int getMaxBandCacheSize() {
        return maxBandCacheSize;
    }

    public void setMaxBandCacheSize(int maxBandCacheSize) {
        this.maxBandCacheSize = maxBandCacheSize;
    }

    public ByteArrayOutputStream pullDataItems(long min, long max) throws Exception {
        return verLogger.rangeVersionHistory(this, min, max);
    }

    public void set_state_word(int state_word) throws Exception {
        log.info("SortBandListFactory set_state_word : " + state_word);
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

    private void takeSnapShot() {
        synchronized (globalLocker) {
            if (dirtyBandSnapShot.size() < 1) {
                dirtyBandSnapShot.clear();
                for (Iterator it = dirtyBands.values().iterator(); it.hasNext(); ) {
                    Band band = (Band) it.next();
                    dirtyBandSnapShot.put(band.getId(), band.clone());
                    it.remove();
                }
                dirtyBands.clear();
            } else {
                log.info("takeSnapShot() dirtyBandSnapShot not empty .... ");
            }
            if (dirtyHeadBandsSnapShot.size() < 1) {
                dirtyHeadBandsSnapShot.clear();
                for (Iterator it = dirtyHeadBands.values().iterator(); it.hasNext(); ) {
                    Band band = (Band) it.next();
                    dirtyHeadBandsSnapShot.put(band.getListName(), band.clone());
                    it.remove();
                }
                dirtyHeadBands.clear();
            } else {
                log.warning("takeSnapShot() dirtyHeadBandsSnapShot not empty .... ");
            }
        }
    }

    public DataSource getIdDataSource() {
        return idDataSource;
    }

    public void setIdDataSource(DataSource idDataSource) {
        this.idDataSource = idDataSource;
    }

    public PlatformTransactionManager getIdTxManager() {
        return idTxManager;
    }

    public void setIdTxManager(PlatformTransactionManager idTxManager) {
        this.idTxManager = idTxManager;
    }

    public String getFactoryName() {
        return factoryName;
    }

    public void setFactoryName(String factoryName) {
        this.factoryName = factoryName;
    }

    public String getLogDirectory() {
        return logDirectory;
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

    public PlatformTransactionManager getTxManager() {
        return txManager;
    }

    public void setTxManager(PlatformTransactionManager txManager) {
        this.txManager = txManager;
    }

    public Object getGlobalLocker() {
        return globalLocker;
    }

    private Object getListLocker(String key) {
        return listMutex.getMutex(key);
    }

    public IBandSerializer getBandSerializer() {
        return bandSerializer;
    }

    public void setBandSerializer(IBandSerializer bandSerializer) {
        this.bandSerializer = bandSerializer;
    }

    public Map getBandcache() {
        return bandCache;
    }

    public Map getListcache() {
        return listCache;
    }

    private String getOperationLogFileName() {
        if (!logDirectory.endsWith("/") && (!logDirectory.endsWith("\\"))) {
            logDirectory += "/";
        }
        return logDirectory + factoryName + ".log";
    }

    private String getOldOperationLogFileName() {
        if (!logDirectory.endsWith("/") && (!logDirectory.endsWith("\\"))) {
            logDirectory += "/";
        }
        return logDirectory + factoryName + ".oldlog";
    }

    private String getBandLogFileName() {
        if (!logDirectory.endsWith("/") && (!logDirectory.endsWith("\\"))) {
            logDirectory += "/";
        }
        return logDirectory + factoryName + ".band";
    }

    public IListBandDao getDao() {
        return dao;
    }

    public SortBandListFactory() {
        listCache = Collections.synchronizedMap(new SoftHashMap());
        bandCache = Collections.synchronizedMap(new SoftHashMap());
        dirtyBands = new ConcurrentHashMap();
        this.dirtyBandSnapShot = new ConcurrentHashMap();
        dirtyHeadBands = new ConcurrentHashMap();
        dirtyHeadBandsSnapShot = new ConcurrentHashMap();
        //flusherWaiter = new Object();
        stateMonitor = new Object();
    }

    synchronized public void stop() throws InterruptedException {
    }

    public void setDao(IListBandDao dao) {
        this.dao = dao;
    }

    int calculateSize(Band band) {
        assert (band.getMeta() == 1);
        int size = 0;
        for (Band.BandInfo info : band.getBandInfos()) {
            size += info.getNumber();
        }
        return size;
    }

    /*private int merge_data_band(Band mband) {
        int reduce = 0;

        synchronized (globalLocker) {
            if (mband.getMeta() != 1) {
                return reduce;
            }
            try {
                long min = getMaxObjectsPerBand() / 10;
                long max = getMaxObjectsPerBand() / 2;
                int start_pos = 0;
                List<Band.BandInfo> bandinfos = mband.getBandInfos();

                while (start_pos < bandinfos.size()) {
                    long size = 0;
                    int first_pos = start_pos;
                    int last_pos = -1;

                    for (int i = first_pos; i < bandinfos.size(); i++) {
                        Band.BandInfo info = bandinfos.get(i);
                        if (info.getNumber() < min) {
                            if (last_pos == -1) {
                                first_pos = i;
                            }
                            last_pos = i;
                        } else {
                            break;
                        }
                        size += info.getNumber();
                        if (size >= max) {
                            break;
                        }
                    }
                    if (last_pos > first_pos) {
                        String min_key = bandinfos.get(first_pos).getMinKey();
                        String max_key = bandinfos.get(last_pos).getMaxKey();
                        Vector<SortListObject> newObjList = new Vector<SortListObject>();
                        for (int i = first_pos; i <= last_pos; i++) {
                            Band.BandInfo info = bandinfos.get(i);
                            Band band = getBandById(mband.getListName(), info.getBandId());
                            newObjList.addAll(band.getObjList().subList(0, (int) info.getNumber()));
                        }
                        Band newBand = new Band();
                        newBand.setListName(mband.getListName());
                        newBand.setMeta(0);
                        newBand.setHead(0);
                        newBand.setObjList(newObjList);
                        newBand.setMinKey(min_key);
                        newBand.setMaxKey(max_key);
                        newBand.setNextMetaBandId(0);
                        newBand.setPrevMetaBandId(0);
                        newBand.setId(getNewBandId());
                        putToDirtyBandList(newBand);
                        Band.BandInfo newinfo = new Band.BandInfo();
                        newinfo.setBandId(newBand.getId());
                        newinfo.setMinKey(min_key);
                        newinfo.setMaxKey(max_key);
                        newinfo.setNumber(size);
                        Vector<Band> oldbands = new Vector<Band>();
                        for (int i = first_pos; i <= last_pos; i++) {
                            Band.BandInfo info = bandinfos.get(first_pos);
                            Band band = getBandById(mband.getListName(), info.getBandId());
                            oldbands.add(band);
                            bandinfos.remove(first_pos);
                            ++reduce;
                        }
                        bandinfos.add(first_pos, newinfo);
                        putToDirtyBandList(mband);
                        for (Band bd : oldbands) {
                            bd.getObjList().clear();
                            putToDirtyBandList(bd);
                        }
                    }
                    start_pos = first_pos + 1;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (reduce > 0) {
            System.out.println("merge data band number : " + reduce);
        }

        return reduce;
    }

    private int merge_meta_band(List<Band> metaBands) {
        int reduce = 0;

        synchronized (globalLocker) {
            if (metaBands.get(0).getMeta() != 1) {
                return reduce;
            }
            try {
                long min = getMaxObjectsPerBand() / 10;
                long max = getMaxObjectsPerBand() / 2;
                int start_pos = 0;

                while (start_pos < metaBands.size()) {
                    long size = 0;
                    int first_pos = start_pos;
                    int last_pos = -1;

                    for (int i = first_pos; i < metaBands.size(); i++) {
                        List<Band.BandInfo> infos = metaBands.get(i).getBandInfos();
                        if (infos.size() < min) {
                            if (last_pos == -1) {
                                first_pos = i;
                            }
                            last_pos = i;
                        } else {
                            break;
                        }
                        size += infos.size();
                        if (size >= max) {
                            break;
                        }
                    }
                    if (last_pos > first_pos) {
                        Vector<Band.BandInfo> newinfos = new Vector<Band.BandInfo>();
                        for (int i = first_pos; i <= last_pos; i++) {
                            List<Band.BandInfo> infos = metaBands.get(i).getBandInfos();
                            newinfos.addAll(infos);
                        }
                        Band keep = metaBands.get(first_pos);
                        keep.setBandInfos(newinfos);
                        keep.setMaxKey(metaBands.get(last_pos).getMaxKey());
                        keep.setNextMetaBandId(metaBands.get(last_pos).getNextMetaBandId());
                        if (metaBands.size() > last_pos + 1) {
                            Band next = metaBands.get(last_pos + 1);
                            next.setPrevMetaBandId(keep.getId());
                            putToDirtyBandList(next);
                        }
                        Vector<Band> oldbands = new Vector<Band>();
                        for (int i = first_pos + 1; i <= last_pos; i++) {
                            Band band = metaBands.get(first_pos + 1);
                            oldbands.add(band);
                            metaBands.remove(first_pos + 1);
                            ++reduce;
                        }
                        putToDirtyBandList(keep);
                        for (Band bd : oldbands) {
                            bd.getBandInfos().clear();
                            putToDirtyBandList(bd);
                        }
                    }
                    start_pos = first_pos + 1;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (reduce > 0) {
            System.out.println("merge meta band number : " + reduce);
        }

        return reduce;
    }

    private int merge_band(List<Band> metaBands) {
        int reduce = 0;

        synchronized (globalLocker) {
            try {
                for (Band bd : metaBands) {
                    reduce += merge_data_band(bd);
                }
                if (reduce > 0) {
                    System.out.println("total merge data band : " + reduce);
                }
                reduce += merge_meta_band(metaBands);
                if (reduce > 0) {
                    if (logfos == null) {
                        logfos = new FileOutputStream(getOperationLogFileName(), true);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return reduce;
    }*/

    private void putListToListCache(SortBandList sbList) {
        listCache.put(sbList.listId, sbList);
    }

    public ISortList getList(String listId, boolean create) throws Exception {
        if (listId.getBytes().length > 120) {
            throw new Exception("listId too long , name = " + listId);
        }
        if (listId.indexOf(";") >= 0 || listId.indexOf(",") >= 0) {
            throw new Exception("listId can not contains ';' name = " + listId);
        }
        synchronized (getListLocker(listId)) {
            SortBandList list = (SortBandList) this.getListcache().get(listId);
            if (list != null) {
                return list;
            }
            if (list == null) {
                list = new SortBandList();
                Band hband = this.getHeadBand(listId);
                if (hband == null) {
                    if (create) {
                        hband = new Band();
                        hband.setHead(1);
                        hband.setMeta(0);
                        hband.setId(this.getNewBandId());
                        hband.setListName(listId);
                        list.setHeadBand(hband);
                        list.setSize(0);
                        list.setBandService(this);
                        list.setListId(listId);
                        putListToListCache(list);
                        return list;
                    } else {
                        return null;
                    }
                } else {
                    if (hband.getMeta() == 1) {
                        list.setHeadBand(hband);
                        int size = 0;
                        Band curBand = hband;
                        Vector<Band> metaBands = new Vector<Band>();
                        metaBands.add(hband);
                        size += calculateSize(curBand);
                        while (curBand.getNextMetaBandId() > 0) {
                            curBand = this.getBandById(list, curBand.getNextMetaBandId());
                            metaBands.add(curBand);
                            size += calculateSize(curBand);
                        }
                        list.setSize(size);
                        { // WPF
                            // merge_band(metaBands);
                        }
                        list.setMetaBands(metaBands);
                        list.setBandService(this);
                        list.setListId(listId);
                        putListToListCache(list);
                        return list;
                    } else {
                        list.setHeadBand(hband);
                        list.setListId(listId);
                        list.setSize(hband.getObjList().size());
                        list.setMetaBands(null);
                        list.setBandService(this);
                        putListToListCache(list);
                        return list;
                    }
                }
            } else {
                return list;
            }
        }
    }

    public ISortList createList(String listId) throws Exception {
        SortBandList list = new SortBandList();
        Band hband;
        hband = new Band();
        hband.setHead(1);
        hband.setMeta(0);
        hband.setId(this.getNewBandId());
        hband.setListName(listId);
        list.setHeadBand(hband);
        list.setSize(0);
        list.setBandService(this);
        list.setListId(listId);
        putListToListCache(list);
        return list;
    }

    boolean bInitialized = false;

    public synchronized void init() {
        if (bInitialized) {
            return;
        } else {
            bInitialized = true;
        }
        boolean verInit = false;
        try {
            verLogger = new VersionHistoryLogger();
            verLogger.setLoggerDirectory(logDirectory + "/share");
            verLogger.setDs(ds);
            verLogger.setVersionTableName("t_pigeontransaction");
            verLogger.setVersionKeyName(versionKeyName);
            verInit = verLogger.init();
            {
                maxBandID = getMaxBandIdFromDB(((ListBandDao) dao).getTableName());
                if (maxBandID < 0) {
                    System.out.println("list maxBandID < 0 ...... exit ");
                    verInit = false;
                } else {
                    if (maxBandID < 50000) {
                        maxBandID = 50000L;
                    }
                }
                System.out.println("list db max band id ... maxBandID = " + maxBandID);
            }
        } catch (Exception e) {
            e.printStackTrace();
            verInit = false;
        }
        try {
            if (!verInit) {
                log.warning("VersionHistoryLogger init failed");
                set_state_word(Constants.READONLY_STATE);
                System.exit(-1);
            }
            this.replayOldOpLog();
            this.replayNewOpLog();
            flusher = new Flusher();
            Thread t = new Thread(flusher);
            t.start();
            verLogger.reloadVersion();
            PersistenceService.syncVersionTask(this);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Band getBandById(ISortList list, long bandid) throws Exception {
        synchronized (list) {
            Band b = (Band) getBandcache().get(bandid);
            if (b != null) {
                return b;
            }
            if (b == null) {
                b = (Band) this.dirtyBands.get(bandid);
                if (b != null) {
                    Band cloneBand = b.clone();
                    getBandcache().put(bandid, cloneBand);
                    return cloneBand;
                }
                b = (Band) this.dirtyBandSnapShot.get(bandid);
                if (b != null) {
                    Band cloneBand = b.clone();
                    getBandcache().put(bandid, cloneBand);
                    return cloneBand;
                }
                b = dao.getBandById(bandid);
                if (b == null) {
                    return null;
                }
                this.bandSerializer.parseValue(b);
                if (b != null) {
                    getBandcache().put(bandid, b);
                } else {
                    throw new Exception("band " + bandid + " does not exists.");
                }
            }
            if (b == null) {
                System.out.println("bandid=" + bandid + " does not exists.");
            }
            return b;
        }
    }

    public Band getHeadBand(String listId) throws Exception {
        synchronized (getListLocker(listId)) {
            Band b = (Band) dirtyHeadBands.get(listId);
            if (b != null) {
                return b.clone();
            }
            b = (Band) dirtyHeadBandsSnapShot.get(listId);
            if (b != null) {
                return b.clone();
            }
            b = dao.getHeadBand(listId);
            if (b == null) {
                return null;
            }
            this.bandSerializer.parseValue(b);
            if (b != null) {
                getBandcache().put(b.getId(), b);
            }
            return b;
        }
    }

    synchronized private long getMaxBandIdFromDB(String tableName) throws Exception {
        System.out.println("getMaxBandIdFromDB tableName = " + tableName);
        long max = -1L;
        Connection _conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            DataSource ds = this.idDataSource;
            _conn = ds.getConnection();
            stmt = _conn.prepareStatement("select max(id) as c from " + tableName);
            rs = stmt.executeQuery();
            if (rs.next()) {
                int c = rs.getInt(1);
                max = c;
            } else {
                max = 0L;
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (_conn != null && _conn.isClosed() == false) {
                _conn.close();
            }
        }
        return max;
    }

    private int getNewId(String Name) throws Exception {
        int max = 0;
        synchronized (maxBandID) {
            ++maxBandID;
            max = maxBandID.intValue();
        }
        return max;
    }

    boolean isFull(){
        return(dirtyBands.size()+dirtyHeadBands.size()) > 10000;
    }
    public DataItem writeLogAndDuplicate(String s) throws Exception {
        if (!Constants.canWriteLog(state_word)) {
            throw new Exception("pigeon is READONLY ...... ");
        }
        if(isFull()){
           log.info("add list too fast");
        }
        DataItem di = null;
        if (DuplicateService.syncQueueOverflow(this)) {
            throw new Exception("list DuplicateService.syncQueueOverflow() ... check sync copy or flush db ... ");
        }
        synchronized (globalLocker) {
            if (logfos == null) {
                logfos = new FileOutputStream(getOperationLogFileName(), true);
            }
            byte[] data = s.getBytes("UTF-8");
            long ver = verLogger.logVersionHistory(data, logfos);
            if (ver < 1) {
                throw new Exception("VersionHistoryLogger version < 1 ...... ");
            }
            logfos.flush();
            if (DuplicateService.isMaster(this)) {
                di = DuplicateService.duplicateData(this, ver, data);
            }
        }
        return di;
    }

    private void addStringToCache(String s) throws Exception {
        s = s.replace("\n", "");
        String fields[] = s.split(",");
        SortBandList list = null;
        try {
            if (fields[0].equals(SortBandList.OP_ADD)) {
                String listId = fields[1];
                String objid = fields[2];
                String key = "";
                if (fields.length > 3) {
                    key = fields[3];
                }
                list = (SortBandList) getList(listId, true);
                boolean result = false;
                result = list.internal_add(new SortListObject(key, objid));
                if (!result) {
                    System.out.println(listId + " addStringToCache sortObj list add failed ... return == false");
                }
            } else if (fields[0].equals(SortBandList.OP_DELETE)) {
                String listId = fields[1];
                String objid = fields[2];
                String key = "";
                if (fields.length > 3) {
                    key = fields[3];
                }
                list = (SortBandList) getList(listId, true);
                boolean result = false;
                result = list.internal_delete(new SortListObject(key, objid));
                if (!result) {
                    System.out.println(listId + " addStringToCache sortObj list delete failed ... return == false");
                }
            } else if (fields[0].equals(SortBandList.OP_REORDER)) {
                String listId = fields[1];
                String old_objid = fields[2];
                String old_key = fields[3];
                String new_objid = fields[4];
                String new_key = "";
                if (fields.length > 5) {
                    new_key = fields[5];
                }
                list = (SortBandList) getList(listId, true);
                boolean result = false;
                result = list.internal_reorder(new SortListObject(old_key, old_objid), new SortListObject(new_key, new_objid));
                if (!result) {
                    System.out.println(listId + " addStringToCache sortObj list reorder failed ... return == false");
                }
            }
        } finally {
        }
    }

    private void writeLogAndCacheRaw(long version, String s) throws Exception {
        if (!Constants.canWriteLog(state_word)) {
            throw new Exception("pigeon is READONLY ...... ");
        }
        synchronized (globalLocker) {
            byte[] data = s.getBytes("UTF-8");
            if (logfos == null) {
                logfos = new FileOutputStream(getOperationLogFileName(), true);
            }
            long delta = verLogger.getVersionDistance(version);
            if (delta < 1) {
                System.out.println("writeLogAndCacheRaw delta = " + delta + ", version = " + version);
                return;
            } else if (delta > 1) {
                throw new Exception("critic! writeLogAndCacheRaw delta = " + delta + ", version = " + version);
            }
            long ver = verLogger.logVersionAndData(version, data, logfos);
            if (ver < 1) {
                throw new Exception("VersionHistoryLogger version < 1 ...... ");
            }
            logfos.flush();
            addStringToCache(s);
        }
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
                    byte[] buf = null;
                    try {
                        v = CommonTools.readLong(is);
                        buf = CommonTools.readBytes(is);
                        ++verNum;
                    } catch (Exception e) {
                        break;
                    }
                    try {
                        String line = new String(buf, "UTF-8");
                        writeLogAndCacheRaw(v, line);
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
        System.out.println(TimeTools.getNowTimeString() + " list syncVersion begin = " + begin + ", end = " + end + ", verNum = " + verNum + ", syncNum = " + syncNum);
    }

    public void writeLogAndCache(long version, String s) throws Exception {
        if (!Constants.canWriteLog(state_word)) {
            throw new Exception("pigeon is READONLY ...... ");
        }
        byte[] data = s.getBytes("UTF-8");
        synchronized (globalLocker) {
            if (logfos == null) {
                logfos = new FileOutputStream(getOperationLogFileName(), true);
            }
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
            long ver = verLogger.logVersionAndData(version, data, logfos);
            if (ver < 1) {
                throw new Exception("VersionHistoryLogger version < 1 ...... ");
            }
            logfos.flush();
            addStringToCache(s);
        }
    }

    public int getMaxObjectsPerBand() {
        return maxObjectsPerBand;
    }

    public void setMaxObjectsPerBand(int maxObjectsPerBand) {
        this.maxObjectsPerBand = maxObjectsPerBand;
    }

    public int getMaxBandInfosPerBand() {
        return maxBandInfosPerBand;
    }

    public void setMaxBandInfosPerBand(int maxBandInfosPerBand) {
        this.maxBandInfosPerBand = maxBandInfosPerBand;
    }

    public void putToDirtyBandList(Band band) throws Exception {
        synchronized (this.globalLocker) {
            Band cloneBand = band.clone();
            dirtyBands.put(band.getId(), cloneBand);
            if (band.getHead() == 1) {
                dirtyHeadBands.put(band.getListName(), cloneBand);
            }
        }
//        synchronized (flusherWaiter) {
//            flusherWaiter.notify();
//        }
    }

    public long getNewBandId() throws Exception {
        return this.getNewId(factoryName + "piglist");
    }

    private boolean renameOpLog() {
        synchronized (globalLocker) {
            try {
                if (logfos != null) {
                    try {
                        logfos.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    logfos = null;
                }
                File fo = new File(getOldOperationLogFileName());
                if (fo.exists()) {
                    throw new Exception("oldlog file exists , oldlog = " + fo.getAbsolutePath());
                }
                File curLogFile = new File(this.getOperationLogFileName());
                if (curLogFile.exists()) {
                    if (!curLogFile.renameTo(new File(this.getOldOperationLogFileName()))) {
                        throw new Exception("renameTo failed , name = " + fo.getAbsolutePath());
                    }
                }
                return true;
            } catch (Throwable t) {
                t.printStackTrace();
            }
            return false;
        }
    }

    private void deleteOldOpLog() {
        boolean isOK = false;
        try {
            if (verLogger.rotateVersionHistory(this, this.getOldOperationLogFileName())) {
                File oldFile = new File(this.getOldOperationLogFileName());
                oldFile.delete();
                isOK = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (!isOK) {
            log.warning("rotateVersionHistory failed READONLY ... ");
            try {
                set_state_word(Constants.READONLY_STATE);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void internal_replayOpLog(File f) throws Exception {
        int lineNum = 1;
        FileInputStream fis = new FileInputStream(f);
        while (true) {
            byte[] bytes = verLogger.getBytesFromVersionHistoryFile(fis);
            if (bytes == null) {
                break;
            }
            String line = new String(bytes, "UTF-8");
            line = line.replace("\n", "");
            if (lineNum % 10000 == 0) {
                log.log(Level.INFO, "lineNum=" + lineNum);
            }
            lineNum++;
            try {
                String fields[] = line.split(",");
                if (fields[0].equals(SortBandList.OP_ADD)) {
                    do_add(fields);
                } else if (fields[0].equals(SortBandList.OP_DELETE)) {
                    do_delete(fields);
                } else if (fields[0].equals(SortBandList.OP_REORDER)) {
                    do_reorder(fields);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (fis != null) {
            fis.close();
        }
        this.takeSnapShot();
        if (!this.flushSnapShotToDataBase()) {
            throw new Exception("flushSnapShotToDataBase list failed");
        }
    }

    private void replayOldOpLog() throws Exception {
        File f = new File(this.getOldOperationLogFileName());
        if (f.exists()) {
            try {
                internal_replayOpLog(f);
                deleteOldOpLog();
            } catch (Exception e) {
                e.printStackTrace();
                log.warning("list replayOldOpLog failed ..... ");
                set_state_word(Constants.READONLY_STATE);
                System.exit(-1);
            }
        }
    }

    private void replayNewOpLog() throws Exception {
        if (!this.renameOpLog()) {
            throw new Exception("list replayNewOpLog failed ............... ");
        }
        replayOldOpLog();
    }

    private void do_reorder(String[] fields) throws Exception {
        String listId = fields[1];
        SortBandList list = null;
        try {
            list = (SortBandList) this.getList(listId, true);
            SortListObject oldobj = new SortListObject();
            oldobj.setObjid(fields[2]);
            oldobj.setKey(fields[3]);
            SortListObject newobj = new SortListObject();
            newobj.setObjid(fields[4]);
            if (fields.length > 5) {
                newobj.setKey(fields[5]);
            } else {
                newobj.setKey("");
            }
            list.internal_reorder(oldobj, newobj);
        } finally {
        }
    }

    private void do_delete(String[] fields) throws Exception {
        String listId = fields[1];
        SortBandList list = null;
        try {
            list = (SortBandList) this.getList(listId, true);
            SortListObject obj = new SortListObject();
            obj.setObjid(fields[2]);
            if (fields.length > 3) {
                obj.setKey(fields[3]);
            } else {
                obj.setKey("");
            }
            list.internal_delete(obj);
        } finally {
        }
    }

    private void do_add(String[] fields) throws Exception {
        String listId = fields[1];
        SortBandList list = null;
        try {
            list = (SortBandList) this.getList(listId, true);
            SortListObject obj = new SortListObject();
            obj.setObjid(fields[2]);
            if (fields.length > 3) {
                obj.setKey(fields[3]);
            } else {
                obj.setKey("");
            }
            list.internal_add(obj);
        } finally {
        }
    }

    boolean noDirtyCache() {
        synchronized (globalLocker) {
            if (state_word == Constants.NOWRITEDB_STATE && dirtyBandSnapShot.size() == 0) {
                return true;
            }
            if (dirtyBands.size() == 0 && dirtyBandSnapShot.size() == 0) {
                return true;
            }
            return false;
        }
    }

    class Flusher implements Runnable {

        boolean isFlushing = false;

        public boolean isFlushing() {
            return isFlushing;
        }

        public void setFlushing(boolean flushing) {
            isFlushing = flushing;
        }

        Object flushOnceLocker = new Object();

        private void flush() {
            log.info("entering flush");
            synchronized (flushOnceLocker) {
                log.log(Level.INFO,"after enter flushOnceLocker");
                synchronized (globalLocker) {
                    log.log(Level.FINE,"after enter globalLocker");
                    if ((dirtyBands.size() == 0) && (dirtyHeadBands.size() == 0)) {
                        log.log(Level.FINE,"dirtyBands.size() == 0 && dirtyHeadBands.size() == 0 exiting flush");
                        return;
                    }
                    if (dirtyBandSnapShot.size() == 0) {
                        if (renameOpLog()) {
                            takeSnapShot();
                        }
                    }
                    else{
                        log.log(Level.INFO,"dirtyBandSnapShot.size() exiting flush");
                    }
                }
                try {
                    if (flushSnapShotToDataBase()) {
                        deleteOldOpLog();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return;
                }
            }
        }

        public void run() {
            flusherStopped = false;
            Thread.currentThread().setName("SortBandListFactory_Flusher_run");
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
                            System.out.println(TimeTools.getNowTimeString() + " List " + cs);
                        }
                    }
//                    if (waiting) {
////                        synchronized (flusherWaiter) {
////                            flusherWaiter.wait(1000);
////                        }
//                        try {
//                            if (dirtyBands.size() == 0 && dirtyHeadBands.size()==0) {
//                                Thread.sleep(1000);
//                            }
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                        synchronized (stateMonitor) {
//                            if (stateChange && !Constants.canWriteDB(state_word)) {
//                                continue;
//                            }
//                        }
//                    }
                    try {
                        synchronized (globalLocker) {
                            if (dirtyBands.size() == 0 && dirtyHeadBands.size()==0) {
                                globalLocker.wait(1000);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
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

    Object flushDbOnceLocker = new Object();

    private boolean flushSnapShotToDataBase() throws Exception {
        log.info("enter flushSnapShotToDataBase");
        synchronized (flushDbOnceLocker) {
            DefaultTransactionDefinition def = new DefaultTransactionDefinition();
            def.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
            Connection conn = ds.getConnection();
            int size = dirtyBandSnapShot.size();
            if (size < 1) {
                return true;
            }
            long lastVersion = -1L;
            TransactionStatus status = txManager.getTransaction(def);
            boolean isOk = false;
            String debug = "before for ... ";
            int order = 0;
            long begin = System.currentTimeMillis();
            try {
                Band b = null;

                    for (Iterator it = dirtyBandSnapShot.values().iterator(); it.hasNext(); ) {
                        b = (Band) it.next();
                        if (dao.isExists(b.getId())) {
                            b.setValue(null);
                            if (b.isEmpty()) {
                                dao.deleteById(b.getId());
                            } else {
                                dao.updateBand(b);
                            }
                        } else {
                            if (!b.isEmpty()) {
                                dao.insertBand(b);
                            }
                        }
                        ++order;
                    }

                    lastVersion = 0L;

                    long[] minmax = verLogger.getMinMaxVersion(getOldOperationLogFileName());
                    if (minmax != null) {
                        lastVersion = minmax[1];
                    } else {
                        System.out.println("list flush minmax == null");
                    }

                debug = "before lastVersion ... ";
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
                }
                isOk = true;
                long end = System.currentTimeMillis();
                log.info("sort list flush to db, band number=" + order+" time=" + (end-begin) + "ms");
                debug = "try over ... ";
            }
            catch(Exception e){
                log.log(Level.SEVERE,"fatal,can not insert into db.",e);
                set_state_word(Constants.READONLY_STATE);
            }
            finally {
                long end = System.currentTimeMillis();
                log.info("sort list flush to db, band number=" + order+" time=" + (end-begin) + "ms");
                if (isOk) {
                    txManager.commit(status);
                    synchronized (globalLocker) {
                        dirtyBandSnapShot.clear();
                        dirtyHeadBandsSnapShot.clear();
                    }
                    savedbfailedcount = 0;

                } else {
                    txManager.rollback(status);
                    log.warning("list flushSnapShotToDataBase failed, fail count : " + savedbfailedcount);
                    ++savedbfailedcount;
                    if (savedbfailedcount > 1800) {
                        //log.warning("list flushSnapShotToDataBase savedbfailedcount : " + savedbfailedcount);
                        set_state_word(Constants.READONLY_STATE);
                    }
                    throw new Exception("can not insert to database; debug = " + debug + ", order = " + order);
                }
            }
            if (lastVersion < 1) {
                System.out.println("list panic!!! (lastVersion < 1) = " + lastVersion);
            }
            return isOk;
        }
    }

}

