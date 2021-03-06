package net.xinshi.pigeon.saas;

import net.xinshi.pigeon.resourcelock.IResourceLock;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-8-20
 * Time: 下午3:36
 * To change this template use File | Settings | File Templates.
 */

public class MerchantResourceLock extends SaasMerchant implements IResourceLock {

    IResourceLock rawResourceLock;

    public MerchantResourceLock(String merchantId, IResourceLock lock) {
        super(merchantId, null);
        this.rawResourceLock = lock;
    }

    @Override
    public void Lock(String resId) throws Exception {
        rawResourceLock.Lock(getKey(resId));
    }

    @Override
    public void Unlock(String resId) throws Exception {
        rawResourceLock.Unlock(getKey(resId));
    }

}

