package net.xinshi.pigeon.client.distributedclient.lockclient;

import net.xinshi.pigeon.client.minaclient.AsynLockClient;
import net.xinshi.pigeon.distributed.bean.HashRange;
import net.xinshi.pigeon.distributed.bean.PigeonNode;
import net.xinshi.pigeon.distributed.client.NodesDispatcher;
import net.xinshi.pigeon.distributed.util.DefaultHashGenerator;
import net.xinshi.pigeon.resourcelock.IResourceLock;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-7
 * Time: 下午3:22
 * To change this template use File | Settings | File Templates.
 */

public class DistributedLock implements IResourceLock {

    String type = "lock";

    private NodesDispatcher nodesDispatcher = null;

    HashMap<String, AsynLockClient> mapLocks = new HashMap<String, AsynLockClient>();

    Logger logger = Logger.getLogger(DistributedLock.class.getName());


    public DistributedLock(NodesDispatcher nodesDispatcher) {
        this.nodesDispatcher = nodesDispatcher;
    }

    public void init() throws Exception {
        System.out.println("distributed flexobject init ...... ");
        List<HashRange> listHR = nodesDispatcher.getPigeonTypes().get(type);
        if (listHR == null) {
            return;
        }
        Set<String> endpoint = new TreeSet<String>();
        for (HashRange hr : listHR) {
            for (PigeonNode pn : hr.getMembers()) {
                if (pn.getLock_port() == 0) {
                    continue;
                }
                String key = pn.getLock_host() + ":" + pn.getLock_port();
                if (!endpoint.contains(key)) {
                    endpoint.add(key);
                }
            }
        }
        for (String key : endpoint) {
            String[] parts = key.split(":");
            AsynLockClient alc = new AsynLockClient(parts[0], parts[1]);
            mapLocks.put(key, alc);
        }
    }

    public AsynLockClient hashID(String resId) {
        int hash = DefaultHashGenerator.hash(resId);
        PigeonNode pin = null;
        List<HashRange> listHR = nodesDispatcher.getPigeonTypes().get(type);
        for (HashRange hr : listHR) {
            if (hash >= hr.getLeftBoundary() && hash <= hr.getRightBoundary()) {
                for (PigeonNode pn : hr.getMembers()) {
                    if (pn.getRole() == 'M') {
                        pin = pn;
                        break;
                    }
                }
                break;
            }
        }
        if (pin != null) {
            if (pin.getName().startsWith(type)) {
                AsynLockClient alc = mapLocks.get(pin.getHost() + ":" + pin.getLock_port());
                return alc;
            }
        }
        return null;
    }

    public void Lock(String resId) throws Exception {
        AsynLockClient alc = hashID(resId);
        if (alc == null) {
            throw new Exception("the lock client == null ...... ");
        }
        alc.Lock(resId);
    }

    public void Unlock(String resId) throws Exception {
        AsynLockClient alc = hashID(resId);
        if (alc == null) {
            throw new Exception("the lock client == null ...... ");
        }
        alc.Unlock(resId);
    }

    public void stop() throws Exception {
        for (AsynLockClient alc : mapLocks.values()) {
            alc.setbExit(true);
            alc.stop();
        }
        mapLocks.clear();
    }

}

