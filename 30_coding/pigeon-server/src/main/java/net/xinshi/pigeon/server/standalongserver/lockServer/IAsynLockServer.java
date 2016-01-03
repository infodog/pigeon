package net.xinshi.pigeon.server.standalongserver.lockServer;

/**
 * Created by IntelliJ IDEA.
 * User: zhengxiangyang
 * Date: 11-10-30
 * Time: 上午4:47
 * To change this template use File | Settings | File Templates.
 */
public interface IAsynLockServer {
    boolean lock(String resId,String threadId) throws Exception;
    boolean unlock(String resId, String threadId,String[] nextThreadId) throws Exception;
    boolean removeWaitingThread(String resId, String threadId) throws Exception;
    public String getLocks() throws Exception;

}