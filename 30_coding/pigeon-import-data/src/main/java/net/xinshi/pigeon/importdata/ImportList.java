package net.xinshi.pigeon.importdata;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.list.ISortList;
import net.xinshi.pigeon.list.SortListObject;
import net.xinshi.pigeon.list.bandlist.SortBandList;
import net.xinshi.pigeon.persistence.VersionHistory;
import net.xinshi.pigeon.util.TimeTools;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-6-11
 * Time: 下午2:46
 * To change this template use File | Settings | File Templates.
 */

public class ImportList {
    DataShare ds;
    IPigeonStoreEngine pigeonStoreEngine;
    public long lastVersion = -1;

    public ImportList(config config, IPigeonStoreEngine pigeonStoreEngine) {
        this.pigeonStoreEngine = pigeonStoreEngine;
        ds = new DataShare(config);
    }

    public void init() throws Exception {
        ds.init();
    }

    private void do_add(String[] fields) throws Exception {
        String listId = fields[1];
        ISortList list = null;
        try {
            list = pigeonStoreEngine.getListFactory().getList(listId, true);
            SortListObject obj = new SortListObject();
            obj.setObjid(fields[2]);
            if (fields.length > 3) {
                obj.setKey(fields[3]);
            } else {
                obj.setKey("");
            }
            list.add(obj);
        } finally {
        }
    }

    private void do_reorder(String[] fields) throws Exception {
        String listId = fields[1];
        ISortList list = null;
        try {
            list = pigeonStoreEngine.getListFactory().getList(listId, true);
            SortListObject oldobj = new SortListObject();
            oldobj.setObjid(fields[2]);
            oldobj.setKey(fields[3]);
            SortListObject newobj = new SortListObject();
            newobj.setObjid(fields[4]);
            if (fields.length > 5) {
                newobj.setKey(fields[5]);
            } else {
                newobj.setKey("");
            }
            list.reorder(oldobj, newobj);
        } finally {
        }
    }

    private void do_delete(String[] fields) throws Exception {
        String listId = fields[1];
        ISortList list = null;
        try {
            list = pigeonStoreEngine.getListFactory().getList(listId, true);
            SortListObject obj = new SortListObject();
            obj.setObjid(fields[2]);
            if (fields.length > 3) {
                obj.setKey(fields[3]);
            } else {
                obj.setKey("");
            }
            list.delete(obj);
        } finally {
        }
    }

    public long ImportDataFiles() throws Exception {
        int n = 0;
        while (true) {
            VersionHistory vh = ds.fetchVersionHistory();
            if (vh == null) {
                break;
            }
            if (vh.getVersion() % 10000 == 0) {
                System.out.println(TimeTools.getNowTimeString() + " List import version = " + vh.getVersion());
            }
            String line = new String(vh.getData(), "UTF-8");
            line = line.trim();
            line.replace("\n", "");
            String fields[] = line.split(",");
            while (true) {
                try {
                    if (fields[0].equals(SortBandList.OP_ADD)) {
                        do_add(fields);
                    } else if (fields[0].equals(SortBandList.OP_DELETE)) {
                        do_delete(fields);
                    } else if (fields[0].equals(SortBandList.OP_REORDER)) {
                        do_reorder(fields);
                    } else {
                        System.out.println("Import List panic !");
                    }
                    ++n;
                    break;
                } catch (Exception e) {
                    e.printStackTrace();
                    Thread.sleep(1000);
                }
            }
            lastVersion = vh.getVersion();
        }
        return n;
    }

}

