package net.xinshi.pigeon.server.standalongserver.listServer;

import net.xinshi.pigeon.list.IListFactory;
import net.xinshi.pigeon.list.ISortList;
import net.xinshi.pigeon.list.SortListObject;
import net.xinshi.pigeon.list.bandlist.SortBandListFactory;
import net.xinshi.pigeon.server.standalongserver.BaseServer;
import net.xinshi.pigeon.util.CommonTools;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: zhengxiangyang
 * Date: 11-10-30
 * Time: 上午3:22
 * To change this template use File | Settings | File Templates.
 */
public class ListServer extends BaseServer {
    IListFactory factory;
    Logger logger = Logger.getLogger("ListServer");

    public ListServer() {
        super();
    }

    public IListFactory getFactory() {
        return factory;
    }

    public void setFactory(IListFactory factory) {
        this.factory = factory;
    }




    public void shutDown() {
    }

    public void doGetRange(InputStream in, ByteArrayOutputStream out) throws Exception {
        if (this.isMaster && this.isSuspended) {
            return;
        }
        String listId = CommonTools.readString(in);
        String begin = CommonTools.readString(in);
        String number = CommonTools.readString(in);
        try {
            ISortList list = factory.getList(listId, true);
            int iBegin = Integer.parseInt(begin);
            int iNum = Integer.parseInt(number);
            List<SortListObject> range = list.getRange(iBegin, iNum);
            StringBuilder builder = new StringBuilder();
            for (SortListObject obj : range) {
                builder.append(obj.getKey());
                builder.append(",");
                builder.append(obj.getObjid());
                builder.append(";");
            }
            CommonTools.writeString(out, "ok");
            CommonTools.writeString(out, builder.toString());
        } catch (Exception e) {
            e.printStackTrace();
            CommonTools.writeString(out, e.getMessage());
        }
    }

    public void doDelete(InputStream in, ByteArrayOutputStream out) throws Exception {

        try {
            String listId = CommonTools.readString(in);
            String key = CommonTools.readString(in);
            String objid = CommonTools.readString(in);

            ISortList llist = factory.getList(listId, true);
            llist.delete(new SortListObject(key, objid));
            CommonTools.writeString(out, "ok");
        } catch (Exception e) {
            CommonTools.writeString(out, "error:" + e.getMessage());
            e.printStackTrace();
        }
    }

    public void doAdd(InputStream in, ByteArrayOutputStream out) throws Exception {

        try {
            String listId = CommonTools.readString(in);
            String key = CommonTools.readString(in);
            String objid = CommonTools.readString(in);
            if (key == null || "".equals(key.trim())) {
                throw new Exception(listId + " sortObj key is null");
            }
            if (objid == null || "".equals(objid.trim())) {
                throw new Exception(listId + "sortObj objid is null");
            }

            ISortList llist = factory.getList(listId, true);
            boolean result = llist.add(new SortListObject(key, objid));
            if(result){
                CommonTools.writeString(out, "ok");
            }
            else{
                CommonTools.writeString(out,"ok dup");
            }
        } catch (Exception e) {
            CommonTools.writeString(out, "error:" + e.getMessage());
            // e.printStackTrace();
        }
    }

    public void doIsExists(InputStream in, ByteArrayOutputStream out) throws Exception {

        try {
            String listId = CommonTools.readString(in);
            String key = CommonTools.readString(in);
            String objid = CommonTools.readString(in);
            ISortList llist = factory.getList(listId, true);
            CommonTools.writeString(out, "ok");
            if (llist.isExists(key, objid)) {
                CommonTools.writeString(out, "y");
            } else {
                CommonTools.writeString(out, "n");
            }
        } catch (Exception e) {
            CommonTools.writeString(out, "error:" + e.getMessage());
            e.printStackTrace();
        }
    }

    public void doGetLessOrEqualPos(InputStream in, ByteArrayOutputStream out) throws Exception {
        if (this.isMaster && this.isSuspended) {
            return;
        }
        try {
            String listId = CommonTools.readString(in);
            String key = CommonTools.readString(in);
            String objid = CommonTools.readString(in);
            ISortList llist = factory.getList(listId, true);
            long pos = llist.getLessOrEqualPos(new SortListObject(key, objid));
            CommonTools.writeString(out, "ok");
            CommonTools.writeString(out, "" + pos);
        } catch (Exception e) {
            CommonTools.writeString(out, "error:" + e.getMessage());
            e.printStackTrace();
        }
    }

    public void doGetSortListObject(InputStream in, ByteArrayOutputStream out) throws Exception {
        if (this.isMaster && this.isSuspended) {
            return;
        }
        try {
            String listId = CommonTools.readString(in);
            String key = CommonTools.readString(in);
            ISortList llist = factory.getList(listId, true);
            SortListObject sobj = llist.getSortListObject(key);
            CommonTools.writeString(out, "ok");
            if (sobj == null) {
                CommonTools.writeString(out, "null");
            } else {
                JSONObject jobj = new JSONObject();
                jobj.put("key", sobj.getKey());
                jobj.put("objid", sobj.getObjid());
                CommonTools.writeString(out, jobj.toString());
            }
        } catch (Exception e) {
            CommonTools.writeString(out, "error:" + e.getMessage());
            e.printStackTrace();
        }
    }

    public void doGetSize(InputStream in, ByteArrayOutputStream out) throws Exception {

        try {
            String listId = CommonTools.readString(in);
            ISortList llist = factory.getList(listId, true);
            long size = llist.getSize();
            CommonTools.writeString(out, "ok");
            CommonTools.writeString(out, "" + size);
        } catch (Exception e) {
            CommonTools.writeString(out, "error:" + e.getMessage());
            e.printStackTrace();
        }
    }

    public void doReorder(InputStream in, ByteArrayOutputStream out) throws Exception {
        if (!isMaster()) {
            CommonTools.writeString(out, "not master, can not save data.");
            return;
        }
        if (this.isMaster && this.isSuspended) {
            CommonTools.writeString(out, "aleady suspended,please try it again later!");
            return;
        }
        try {
            String listId = CommonTools.readString(in);
            String old_key = CommonTools.readString(in);
            String old_objid = CommonTools.readString(in);
            String new_key = CommonTools.readString(in);
            String new_objid = CommonTools.readString(in);

            ISortList llist = factory.getList(listId, true);
            llist.reorder(new SortListObject(old_key, old_objid), new SortListObject(new_key, new_objid));
            CommonTools.writeString(out, "ok");
        } catch (Exception e) {
            CommonTools.writeString(out, "error:" + e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void stop() {
        try {
            ((SortBandListFactory)factory).stop();
        } catch (InterruptedException e) {
           logger.log(Level.SEVERE,"can not stop listServer." ,e);
        }
    }
}
