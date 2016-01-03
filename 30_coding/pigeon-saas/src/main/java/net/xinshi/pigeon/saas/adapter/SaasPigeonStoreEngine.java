package net.xinshi.pigeon.saas.adapter;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.atom.IIntegerAtom;
import net.xinshi.pigeon.flexobject.IFlexObjectFactory;
import net.xinshi.pigeon.idgenerator.IIDGenerator;
import net.xinshi.pigeon.list.IListFactory;
import net.xinshi.pigeon.resourcelock.IResourceLock;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-8-24
 * Time: 下午3:01
 * To change this template use File | Settings | File Templates.
 */

public class SaasPigeonStoreEngine implements IPigeonStoreEngine {

    IFlexObjectFactory flexObjectFactory;
    IIntegerAtom atom;
    IListFactory listFactory;
    IIDGenerator idGenerator;
    IResourceLock resourceLock;

    public void setFlexObjectFactory(IFlexObjectFactory flexObjectFactory) {
        this.flexObjectFactory = flexObjectFactory;
    }

    public void setAtom(IIntegerAtom atom) {
        this.atom = atom;
    }

    public void setListFactory(IListFactory listFactory) {
        this.listFactory = listFactory;
    }

    public void setIdGenerator(IIDGenerator idGenerator) {
        this.idGenerator = idGenerator;
    }

    public void setResourceLock(IResourceLock resourceLock) {
        this.resourceLock = resourceLock;
    }

    @Override
    public IFlexObjectFactory getFlexObjectFactory() {
        return flexObjectFactory;
    }

    @Override
    public IIntegerAtom getAtom() {
        return atom;
    }

    @Override
    public IListFactory getListFactory() {
        return listFactory;
    }

    @Override
    public IIDGenerator getIdGenerator() {
        return idGenerator;
    }

    @Override
    public IResourceLock getLock() {
        return resourceLock;
    }

    @Override
    public void stop() throws InterruptedException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

}
