package net.xinshi.pigeon.distributed.bean;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-3
 * Time: 上午11:33
 * To change this template use File | Settings | File Templates.
 */

public class PigeonNode implements Comparable<PigeonNode> {

    String type;
    String range;
    String name;
    String finger;
    String baseurl;
    String host;
    int port;
    String lock_host;
    int lock_port;
    int order;
    char role; // 'M', 'S', 'B'
    long control_version;
    long data_version;
    long lastHeartbeat;
    boolean isActive = false;

    public void assignment(PigeonNode o) {
        setBaseurl(o.getBaseurl());
        setHost(o.getHost());
        setPort(o.getPort());
        if (o.getLock_host().length() > 0) {
            setLock_host(o.getLock_host());
        } else {
            setLock_host(o.getHost());
        }
        setLock_port(o.getLock_port());
        setData_version(o.getData_version());
        setLastHeartbeat(System.currentTimeMillis());
        setActive(true);
    }

    public int compareTo(PigeonNode o) {
        if (this.role == o.role) {
            if (this.order != o.order) {
                return this.order - o.order;
            }
            return this.name.compareTo(o.name);
        }
        if (this.role == 'M' || o.role == 'B') {
            return -1;
        }
        return 1;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getRange() {
        return range;
    }

    public void setRange(String range) {
        this.range = range;
    }

    public String getName() {
        return name;
    }

    public String getFinger() {
        return finger;
    }

    public void setFinger(String finger) {
        this.finger = finger;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getBaseurl() {
        return baseurl;
    }

    public void setBaseurl(String baseurl) {
        this.baseurl = baseurl;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
        if (lock_host == null) {
            lock_host = host;
        }
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public char getRole() {
        return role;
    }

    public void setRole(char role) {
        this.role = role;
    }

    public long getControl_version() {
        return control_version;
    }

    public void setControl_version(long control_version) {
        this.control_version = control_version;
    }

    public long getData_version() {
        return data_version;
    }

    public void setData_version(long data_version) {
        this.data_version = data_version;
    }

    public String getLock_host() {
        return lock_host;
    }

    public void setLock_host(String lock_host) {
        this.lock_host = lock_host;
    }

    public int getLock_port() {
        return lock_port;
    }

    public void setLock_port(int lock_port) {
        this.lock_port = lock_port;
    }

    public long getLastHeartbeat() {
        return lastHeartbeat;
    }

    public void setLastHeartbeat(long lastHeartbeat) {
        this.lastHeartbeat = lastHeartbeat;
    }

    public boolean isActive() {
        return isActive;
    }

    public void setActive(boolean active) {
        isActive = active;
    }

}

