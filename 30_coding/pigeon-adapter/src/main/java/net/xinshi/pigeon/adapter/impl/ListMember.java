package net.xinshi.pigeon.adapter.impl;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.list.IListFactory;
import net.xinshi.pigeon.list.ISortList;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-4-18
 * Time: 下午4:06
 * To change this template use File | Settings | File Templates.
 */

public class ListMember implements IListFactory {

    IPigeonStoreEngine pigeon;

    public IPigeonStoreEngine getPigeon() {
        return pigeon;
    }

    public void setPigeon(IPigeonStoreEngine pigeon) {
        this.pigeon = pigeon;
    }

    public ISortList getList(String listId, boolean create) throws Exception {
        return pigeon.getListFactory().getList(listId, create);
    }

    public ISortList createList(String listId) throws Exception {
        return pigeon.getListFactory().createList(listId);
    }

    public void init() throws Exception {
    }

    public void set_state_word(int state_word) throws Exception {
    }

}
