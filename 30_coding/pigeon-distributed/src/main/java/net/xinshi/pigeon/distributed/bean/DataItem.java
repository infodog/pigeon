package net.xinshi.pigeon.distributed.bean;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-21
 * Time: 上午11:58
 * To change this template use File | Settings | File Templates.
 */

public class DataItem {

    long version;
    byte[] data;
    boolean isComplete = false;

    public DataItem(long version, byte[] data) {
        this.version = version;
        this.data = data;
    }

    public boolean isComplete() {
        return isComplete;
    }

    public void setComplete(boolean complete) {
        isComplete = complete;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public boolean waitme(long timeout) {
        boolean ok = false;
        try {
            synchronized (this) {
                ok = isComplete();
                if (!ok) {
                    this.wait(timeout);
                    ok = isComplete();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ok;
    }

}

