package net.xinshi.pigeon.adapter.impl;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.flexobject.FlexObjectEntry;
import net.xinshi.pigeon.flexobject.IFlexObjectFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-4-18
 * Time: 下午4:09
 * To change this template use File | Settings | File Templates.
 */

public class FlexObjectMember implements IFlexObjectFactory {

    IPigeonStoreEngine pigeon;

    public IPigeonStoreEngine getPigeon() {
        return pigeon;
    }

    public void setPigeon(IPigeonStoreEngine pigeon) {
        this.pigeon = pigeon;
    }

    public String getContent(String name) throws Exception {
        return pigeon.getFlexObjectFactory().getContent(name);
    }

    public void saveContent(String name, String content) throws Exception {
        pigeon.getFlexObjectFactory().saveContent(name, content);
    }

    public List<String> getContents(List<String> names) throws Exception {
        return pigeon.getFlexObjectFactory().getContents(names);
    }

    public void addContent(String name, String value) throws Exception {
        pigeon.getFlexObjectFactory().addContent(name, value);
    }

    public void addContent(String name, byte[] value) throws Exception {
        pigeon.getFlexObjectFactory().addContent(name, value);
    }

    public void saveFlexObject(FlexObjectEntry entry) throws Exception {
        pigeon.getFlexObjectFactory().saveFlexObject(entry);
    }

    public void saveBytes(String name, byte[] content) throws Exception {
        pigeon.getFlexObjectFactory().saveBytes(name, content);
    }

    public byte[] getBytes(String name) throws Exception {
        return pigeon.getFlexObjectFactory().getBytes(name);
    }

    public int deleteContent(String name) throws Exception {
        return pigeon.getFlexObjectFactory().deleteContent(name);
    }

    public List<FlexObjectEntry> getFlexObjects(List<String> names) throws Exception {
        return pigeon.getFlexObjectFactory().getFlexObjects(names);
    }

    public FlexObjectEntry getFlexObject(String name) throws SQLException, Exception {
        return pigeon.getFlexObjectFactory().getFlexObject(name);
    }

    public void saveFlexObjects(List<FlexObjectEntry> objs) throws Exception {
        pigeon.getFlexObjectFactory().saveFlexObjects(objs);
    }

    public void init() throws Exception {
    }

    public void stop() throws Exception {
    }

    public void set_state_word(int state_word) throws Exception {
    }

    public String getConstant(String name) throws Exception {
        return pigeon.getFlexObjectFactory().getConstant(name);
    }

    @Override
    public void setTlsMode(boolean open) {
    }

    @Override
    public void saveTemporaryContent(String name, String content) throws Exception {
    }

}
