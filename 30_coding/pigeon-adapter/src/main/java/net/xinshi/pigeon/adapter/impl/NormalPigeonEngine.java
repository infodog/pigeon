package net.xinshi.pigeon.adapter.impl;

import net.xinshi.pigeon.adapter.IPigeonEngine;
import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.atom.IIntegerAtom;
import net.xinshi.pigeon.filesystem.IFileSystem;
import net.xinshi.pigeon.flexobject.IFlexObjectFactory;
import net.xinshi.pigeon.idgenerator.IIDGenerator;
import net.xinshi.pigeon.list.IListFactory;
import net.xinshi.pigeon.resourcelock.IResourceLock;

/**
 * Created by IntelliJ IDEA.
 * User: mac
 * Date: 11-12-5
 * Time: 下午11:25
 * To change this template use File | Settings | File Templates.
 */
public class NormalPigeonEngine implements IPigeonEngine {

    IFileSystem fileSystem;
    IPigeonStoreEngine pigeonStoreEngine;

    public void setFileSystem(IFileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    public void setPigeonStoreEngine(IPigeonStoreEngine pigeonStoreEngine) {
        this.pigeonStoreEngine = pigeonStoreEngine;
    }

    @Override
    public IFileSystem getFileSystem() {
        return fileSystem;
    }

    public IPigeonStoreEngine getPigeonStoreEngine() {
        return pigeonStoreEngine;
    }

    @Override
    public IFlexObjectFactory getFlexObjectFactory() {
        return pigeonStoreEngine.getFlexObjectFactory();
    }

    @Override
    public IIntegerAtom getAtom() {
        return pigeonStoreEngine.getAtom();
    }

    @Override
    public IListFactory getListFactory() {
        return pigeonStoreEngine.getListFactory();
    }

    @Override
    public IIDGenerator getIdGenerator() {
        return pigeonStoreEngine.getIdGenerator();
    }

    @Override
    public IResourceLock getLock() {
        return pigeonStoreEngine.getLock();
    }

    @Override
    public void stop() throws InterruptedException {
        pigeonStoreEngine.stop();
    }
}
