package net.xinshi.pigeon.adapter;


import net.xinshi.pigeon.atom.IIntegerAtom;
import net.xinshi.pigeon.flexobject.IFlexObjectFactory;
import net.xinshi.pigeon.idgenerator.IIDGenerator;
import net.xinshi.pigeon.list.IListFactory;
import net.xinshi.pigeon.resourcelock.IResourceLock;

/**
 * Created by IntelliJ IDEA.
 * User: zhengxiangyang
 * Date: 11-11-1
 * Time: 下午3:32
 * To change this template use File | Settings | File Templates.
 */
public interface IPigeonStoreEngine {
    IFlexObjectFactory getFlexObjectFactory();
    IIntegerAtom getAtom();
    IListFactory getListFactory();
    IIDGenerator getIdGenerator();

    IResourceLock getLock();

    void stop() throws InterruptedException;
}