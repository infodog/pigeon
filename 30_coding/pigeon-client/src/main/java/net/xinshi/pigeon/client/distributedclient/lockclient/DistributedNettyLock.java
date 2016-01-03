package net.xinshi.pigeon.client.distributedclient.lockclient;

import net.xinshi.pigeon.client.minaclient.IdentityKeygen;
import net.xinshi.pigeon.distributed.Constants;
import net.xinshi.pigeon.distributed.client.NodesDispatcher;
import net.xinshi.pigeon.netty.common.PigeonFuture;
import net.xinshi.pigeon.resourcelock.IResourceLock;
import net.xinshi.pigeon.util.CommonTools;
import org.apache.commons.lang.StringUtils;

import java.io.ByteArrayOutputStream;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-13
 * Time: 下午5:59
 * To change this template use File | Settings | File Templates.
 */

public class DistributedNettyLock implements IResourceLock {

    String type = "lock";

    long LOCK_WAIT = 1000 * 3600;
    long UNLOCK_WAIT = 1000 * 15;

    private NodesDispatcher nodesDispatcher = null;

    Logger logger = Logger.getLogger(DistributedNettyLock.class.getName());

    public DistributedNettyLock(NodesDispatcher nodesDispatcher) {
        this.nodesDispatcher = nodesDispatcher;
    }

    public void init() throws Exception {
        System.out.println("distributed flexobject init ...... ");
    }

    public void Lock(String resId) throws Exception {
        String threadId = IdentityKeygen.get();
        String lockid = "lock$" + resId + "$" + threadId;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, lockid);
        PigeonFuture pf = nodesDispatcher.CommitAsync(type, resId, out);
        if (pf == null) {
            throw new Exception("Lock PigeonFuture == null , resid = " + resId);
        }
        pf.waitme(LOCK_WAIT);
        if (pf.isComplete()) {
            String msg = new String(pf.getData(), Constants.PACKET_PREFIX_LENGTH, pf.getData().length - Constants.PACKET_PREFIX_LENGTH, "UTF-8");
            String[] parts = msg.split("\\$");
            String action = parts[0];
            if (StringUtils.equals(action, "locked")) {
                return;
            }
        }
        throw new Exception("Lock wait time out , resid = " + resId);
    }

    public void Unlock(String resId) throws Exception {
        String threadId = IdentityKeygen.get();
        String lockid = "unlock$" + resId + "$" + threadId;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, lockid);
        PigeonFuture pf = nodesDispatcher.CommitAsync(type, resId, out);
        if (pf == null) {
            throw new Exception("UnLock PigeonFuture == null , resid = " + resId);
        }
        pf.waitme(UNLOCK_WAIT);
        if (pf.isComplete()) {
            String msg = new String(pf.getData(), Constants.PACKET_PREFIX_LENGTH, pf.getData().length - Constants.PACKET_PREFIX_LENGTH, "UTF-8");
            String[] parts = msg.split("\\$");
            String action = parts[0];
            if (StringUtils.equals(action, "unlocked")) {
                return;
            }
        }
        throw new Exception("UnLock error , resid = " + resId);
    }

    public void stop() throws Exception {
        System.out.println("DistributedNettyLock do stop ... ");
    }

}

