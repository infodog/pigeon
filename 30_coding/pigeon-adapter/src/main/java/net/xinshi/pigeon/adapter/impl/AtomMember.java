package net.xinshi.pigeon.adapter.impl;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.atom.IIntegerAtom;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-4-18
 * Time: 下午4:15
 * To change this template use File | Settings | File Templates.
 */

public class AtomMember implements IIntegerAtom {

    IPigeonStoreEngine pigeon;

    public IPigeonStoreEngine getPigeon() {
        return pigeon;
    }

    public void setPigeon(IPigeonStoreEngine pigeon) {
        this.pigeon = pigeon;
    }

    public boolean createAndSet(String name, Integer initValue) throws Exception {
        return pigeon.getAtom().createAndSet(name, initValue);
    }

    public boolean greaterAndInc(String name, int testValue, int incValue) throws Exception {
        return pigeon.getAtom().greaterAndInc(name, testValue, incValue);
    }

    public boolean lessAndInc(String name, int testValue, int incValue) throws Exception {
        return pigeon.getAtom().lessAndInc(name, testValue, incValue);
    }

    public long greaterAndIncReturnLong(String name, int testValue, int incValue) throws Exception {
        return pigeon.getAtom().greaterAndIncReturnLong(name, testValue, incValue);
    }

    public long lessAndIncReturnLong(String name, int testValue, int incValue) throws Exception {
        return pigeon.getAtom().lessAndIncReturnLong(name, testValue, incValue);
    }

    public Long get(String name) throws Exception {
        return pigeon.getAtom().get(name);
    }

    public List<Long> getAtoms(List<String> atomIds) throws Exception {
        return pigeon.getAtom().getAtoms(atomIds);
    }

    public void init() throws Exception {
    }

    public void set_state_word(int state_word) throws Exception {
    }

}
