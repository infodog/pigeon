package net.xinshi.pigeon.adapter.impl;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.resourcelock.IResourceLock;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-2-29
 * Time: 下午5:10
 * To change this template use File | Settings | File Templates.
 */

public class ResourceLockMember implements IResourceLock {

    IPigeonStoreEngine pigeon;

    public IPigeonStoreEngine getPigeon() {
        return pigeon;
    }

    public void setPigeon(IPigeonStoreEngine pigeon) {
        this.pigeon = pigeon;
    }

    public void Lock(String resId) throws Exception {
        pigeon.getLock().Lock(resId);
    }

    public void Unlock(String resId) throws Exception {
        pigeon.getLock().Unlock(resId);
    }

}

