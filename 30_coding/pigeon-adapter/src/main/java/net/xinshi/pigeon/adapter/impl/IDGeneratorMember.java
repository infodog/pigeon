package net.xinshi.pigeon.adapter.impl;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.idgenerator.IIDGenerator;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-14
 * Time: 下午5:44
 * To change this template use File | Settings | File Templates.
 */

public class IDGeneratorMember implements IIDGenerator {

    IPigeonStoreEngine pigeon;

    public IPigeonStoreEngine getPigeon() {
        return pigeon;
    }

    public void setPigeon(IPigeonStoreEngine pigeon) {
        this.pigeon = pigeon;
    }

    @Override
    public long getId(String name) throws Exception {
        return pigeon.getIdGenerator().getId(name);
    }

    public long setSkipValue(String name, long value) throws Exception {
        return pigeon.getIdGenerator().setSkipValue(name, value);
    }

    @Override
    public void set_state_word(int state_word) throws Exception {
    }

}

