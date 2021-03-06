package net.xinshi.pigeon.distributed.bean;

import org.json.JSONObject;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午2:43
 * To change this template use File | Settings | File Templates.
 */

public class ServerConfig {

    String type;
    String nodeName;
    String instanceName;
    String finger;
    long version;
    String table;
    String logDir;
    String baseDir;
    int maxCacheNumber;
    String dbUrl;
    String dbUserName;
    String dbPassword;
    String driverClass;
    String lockHost;
    String lockPort;
    String name;
    String pigeonStoreConfigFile;
    String pigeonFileSystemConfigFile;
    boolean migration = false;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public String getFinger() {
        return finger;
    }

    public void setFinger(String finger) {
        this.finger = finger;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getLogDir() {
        return logDir;
    }

    public void setLogDir(String logDir) {
        this.logDir = logDir;
    }

    public String getBaseDir() {
        return baseDir;
    }

    public void setBaseDir(String baseDir) {
        this.baseDir = baseDir;
    }

    public int getMaxCacheNumber() {
        return maxCacheNumber;
    }

    public void setMaxCacheNumber(int maxCacheNumber) {
        this.maxCacheNumber = maxCacheNumber;
    }

    public String getDbUrl() {
        return dbUrl;
    }

    public void setDbUrl(String dbUrl) {
        this.dbUrl = dbUrl;
    }

    public String getDbUserName() {
        return dbUserName;
    }

    public void setDbUserName(String dbUserName) {
        this.dbUserName = dbUserName;
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public void setDbPassword(String dbPassword) {
        this.dbPassword = dbPassword;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }

    public String getlockHost() {
        return lockHost;
    }

    public void setlockHost(String lockHost) {
        this.lockHost = lockHost;
    }

    public String getlockPort() {
        return lockPort;
    }

    public void setlockPort(String lockPort) {
        this.lockPort = lockPort;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPigeonStoreConfigFile() {
        return pigeonStoreConfigFile;
    }

    public void setPigeonStoreConfigFile(String pigeonStoreConfigFile) {
        this.pigeonStoreConfigFile = pigeonStoreConfigFile;
    }

    public String getPigeonFileSystemConfigFile() {
        return pigeonFileSystemConfigFile;
    }

    public void setPigeonFileSystemConfigFile(String pigeonFileSystemConfigFile) {
        this.pigeonFileSystemConfigFile = pigeonFileSystemConfigFile;
    }

    public boolean isMigration() {
        return migration;
    }

    public static ServerConfig JSONObject2ServerConfig(JSONObject jo) throws Exception {
        ServerConfig sc = new ServerConfig();
        sc.type = jo.optString("type");
        sc.nodeName = jo.optString("nodeName");
        sc.instanceName = jo.optString("instanceName");
        sc.table = jo.optString("table");
        sc.logDir = jo.optString("logDir");
        sc.baseDir = jo.optString("baseDir");
        sc.maxCacheNumber = jo.optInt("maxCacheNumber");
        sc.driverClass = jo.optString("driverClass");
        sc.dbUrl = jo.optString("dbUrl");
        if (sc.dbUrl.indexOf(":oracle:") > 0 && sc.driverClass.isEmpty()) {
            sc.driverClass = "oracle.jdbc.driver.OracleDriver";
        }
        sc.dbUserName = jo.optString("dbUserName");
        sc.dbPassword = jo.optString("dbPassword");
        sc.lockHost = jo.optString("lockHost");
        sc.lockPort = jo.optString("lockPort");
        sc.name = jo.optString("name");
        sc.pigeonStoreConfigFile = jo.optString("pigeonStoreConfigFile");
        sc.pigeonFileSystemConfigFile = jo.optString("pigeonFileSystemConfigFile");
        sc.version = jo.optLong("version");
        sc.migration = jo.optBoolean("migration", false);
        if (sc.migration) {
            sc.maxCacheNumber = (1 << 30);
            System.out.println("!!! migration == true, reset maxCacheNumber = " + sc.maxCacheNumber);
        }
        return sc;
    }

}

