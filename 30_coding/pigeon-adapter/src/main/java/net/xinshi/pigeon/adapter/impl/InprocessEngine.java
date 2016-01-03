package net.xinshi.pigeon.adapter.impl;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.atom.IIntegerAtom;
import net.xinshi.pigeon.flexobject.IFlexObjectFactory;
import net.xinshi.pigeon.idgenerator.IIDGenerator;
import net.xinshi.pigeon.list.IListFactory;
import net.xinshi.pigeon.resourcelock.IResourceLock;

/**
 * Created by IntelliJ IDEA.
 * User: mac
 * Date: 11-11-5
 * Time: 下午5:01
 * To change this template use File | Settings | File Templates.
 */
public class InprocessEngine implements IPigeonStoreEngine {
    @Override
    public IFlexObjectFactory getFlexObjectFactory() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public IIntegerAtom getAtom() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public IListFactory getListFactory() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public IIDGenerator getIdGenerator() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public IResourceLock getLock() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void stop() throws InterruptedException {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
