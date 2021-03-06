package net.xinshi.pigeon.netty.common;

import net.xinshi.pigeon.netty.client.Client;

public class PigeonFuture {

    private boolean isCancel = false;
    private boolean isComplete = false;
    private int sequence = 0;
    private short flag = 0;
    private int index = -1;
    private byte[] data = null;
    private long bornTime = System.currentTimeMillis();
    private Client client = null;

    public boolean isCancel() {
        return isCancel;
    }

    public void setCancel(boolean cancel) {
        isCancel = cancel;
    }

    public boolean isComplete() {
        return isComplete;
    }

    public void setComplete(boolean complete) {
        isComplete = complete;
    }

    public int getSequence() {
        return sequence;
    }

    public void setSequence(int sequence) {
        this.sequence = sequence;
    }

    public short getFlag() {
        return flag;
    }

    public void setFlag(short flag) {
        this.flag = flag;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public long getBornTime() {
        return bornTime;
    }

    public void setBornTime(long bornTime) {
        this.bornTime = bornTime;
    }

    public PigeonFuture(Client client) {
        this.client = client;
    }

    public boolean waitme(long timeout) {
        boolean ok = false;
        try {
            synchronized (this) {
                ok = isComplete();
                if (!ok) {
                    if (isCancel) {
                        return false;
                    }
                    this.wait(timeout);
                    ok = isComplete();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.delPigeonFuture(sequence);
        }
        return ok;
    }

    public String getHost() {
        return client.getHost();
    }

}
