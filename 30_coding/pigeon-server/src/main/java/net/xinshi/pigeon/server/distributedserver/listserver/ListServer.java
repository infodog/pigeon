package net.xinshi.pigeon.server.distributedserver.listserver;

import net.xinshi.pigeon.list.IListFactory;
import net.xinshi.pigeon.list.ISortList;
import net.xinshi.pigeon.list.SortListObject;
import net.xinshi.pigeon.list.bandlist.SortBandListFactory;
import net.xinshi.pigeon.server.distributedserver.BaseServer;
import net.xinshi.pigeon.server.distributedserver.util.Tools;
import net.xinshi.pigeon.util.CommonTools;
import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午5:31
 * To change this template use File | Settings | File Templates.
 */

public class ListServer extends BaseServer {

    IListFactory factory;
    Logger logger = Logger.getLogger(ListServer.class.getName());
    // final static char[] invalidchars = new char[]{',', ';', ':', '\r', '\n'};
    final static char[] invalidchars = new char[]{',', ';'};

    public IListFactory getFactory() {
        return factory;
    }

    public void setFactory(IListFactory factory) {
        this.factory = factory;
    }

    public Map getStatusMap() {
        return ((SortBandListFactory) factory).getStatusMap();
    }

    public void shutDown() {
    }

    public void doGetRange(InputStream in, ByteArrayOutputStream out) throws Exception {
        String listId = CommonTools.readString(in);
        String begin = CommonTools.readString(in);
        String number = CommonTools.readString(in);
        ISortList llist = null;
        try {
            List<SortListObject> range = null;
            int iBegin = Integer.parseInt(begin);
            int iNum = Integer.parseInt(number);
            llist = factory.getList(listId, true);
            range = llist.getRange(iBegin, iNum);
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
            String error = "listId = " + listId + ", exception message = " + e.getMessage();
            System.out.println(error);
            CommonTools.writeString(out, error);
        } finally {
        }
    }

    public void doDelete(InputStream in, ByteArrayOutputStream out) throws Exception {
        ISortList llist = null;
        try {
            String listId = CommonTools.readString(in);
            String key = CommonTools.readString(in);
            String objid = CommonTools.readString(in);
            llist = factory.getList(listId, true);
            llist.delete(new SortListObject(key, objid));
            CommonTools.writeString(out, "ok");
        } catch (Exception e) {
            CommonTools.writeString(out, "error:" + e.getMessage());
            e.printStackTrace();
        } finally {
        }
    }

    public void doAdd(InputStream in, ByteArrayOutputStream out) throws Exception {
        ISortList llist = null;
        try {
            String listId = CommonTools.readString(in);
            Tools.checkNameLength(listId);
            String key = CommonTools.readString(in);
            String objid = CommonTools.readString(in);
            if (key == null || "".equals(key.trim())) {
                throw new Exception(listId + " sortObj key is null");
            }
            if (StringUtils.containsAny(key, invalidchars)) {
                throw new Exception(listId + " sortObj key can't include [, ; : \\r \\n]");
            }
            if (objid == null || "".equals(objid.trim())) {
                throw new Exception(listId + "sortObj objid is null");
            }
            if (StringUtils.containsAny(objid, invalidchars)) {
                throw new Exception(listId + " sortObj objid can't include [, ; : \\r \\n]");
            }
            Tools.checkKeyLength(key);
            Tools.checkKeyLength(objid);
            if (key.length() != key.getBytes().length) {
                CommonTools.writeString(out, "list obj key has invalid chars");
                return;
            }
            if (objid.length() != objid.getBytes().length) {
                CommonTools.writeString(out, "list obj id has invalid chars");
                return;
            }
            boolean result = false;
            llist = factory.getList(listId, true);
            result = llist.add(new SortListObject(key, objid));
            if (result) {
                CommonTools.writeString(out, "ok");
            } else {
                CommonTools.writeString(out, "ok dup");
            }
        } catch (Exception e) {
            CommonTools.writeString(out, "error:" + e.getMessage());
        } finally {
        }
    }

    public void doBatchAdd(InputStream in, ByteArrayOutputStream out) throws Exception {
        ISortList llist = null;
        try {
            String listId = CommonTools.readString(in);
            Tools.checkNameLength(listId);
            List<SortListObject> listSortObj = new ArrayList<SortListObject>();
            while (true) {
                String key = CommonTools.readString(in);
                String objid = CommonTools.readString(in);
                if (key == null || objid == null) {
                    break;
                }
                if ("".equals(key.trim())) {
                    throw new Exception(listId + " sortObj key is null");
                }
                if (StringUtils.containsAny(key, invalidchars)) {
                    throw new Exception(listId + " sortObj key can't include [, ; : \\r \\n]");
                }
                if ("".equals(objid.trim())) {
                    throw new Exception(listId + " sortObj objid is null");
                }
                if (StringUtils.containsAny(objid, invalidchars)) {
                    throw new Exception(listId + " sortObj objid can't include [, ; : \\r \\n]");
                }
                Tools.checkKeyLength(key);
                Tools.checkKeyLength(objid);
                if (key.length() != key.getBytes().length) {
                    CommonTools.writeString(out, "list obj key has invalid chars");
                    return;
                }
                if (objid.length() != objid.getBytes().length) {
                    CommonTools.writeString(out, "list obj id has invalid chars");
                    return;
                }
                listSortObj.add(new SortListObject(key, objid));
            }
            if (listSortObj.size() == 0) {
                throw new Exception(listId + " doBatchAdd no valid SortObj");
            }
            boolean result = false;
            llist = factory.getList(listId, true);
            result = llist.add(listSortObj);
            if (result) {
                CommonTools.writeString(out, "ok");
            } else {
                CommonTools.writeString(out, "doBatchAdd some one SortObj failed : " + listId);
            }
        } catch (Exception e) {
            CommonTools.writeString(out, "error:" + e.getMessage());
        } finally {
        }
    }

    public void doIsExists(InputStream in, ByteArrayOutputStream out) throws Exception {
        ISortList llist = null;
        try {
            String listId = CommonTools.readString(in);
            String key = CommonTools.readString(in);
            String objid = CommonTools.readString(in);
            boolean result = false;
            llist = factory.getList(listId, true);
            result = llist.isExists(key, objid);
            CommonTools.writeString(out, "ok");
            if (result) {
                CommonTools.writeString(out, "y");
            } else {
                CommonTools.writeString(out, "n");
            }
        } catch (Exception e) {
            CommonTools.writeString(out, "error:" + e.getMessage());
            e.printStackTrace();
        } finally {
        }
    }

    public void doGetLessOrEqualPos(InputStream in, ByteArrayOutputStream out) throws Exception {
        ISortList llist = null;
        try {
            String listId = CommonTools.readString(in);
            String key = CommonTools.readString(in);
            String objid = CommonTools.readString(in);
            long pos = -1;
            llist = factory.getList(listId, true);
            pos = llist.getLessOrEqualPos(new SortListObject(key, objid));
            CommonTools.writeString(out, "ok");
            CommonTools.writeString(out, "" + pos);
        } catch (Exception e) {
            CommonTools.writeString(out, "error:" + e.getMessage());
            e.printStackTrace();
        } finally {
        }
    }

    public void doGetSortListObject(InputStream in, ByteArrayOutputStream out) throws Exception {
        ISortList llist = null;
        try {
            String listId = CommonTools.readString(in);
            String key = CommonTools.readString(in);
            SortListObject sobj = null;
            llist = factory.getList(listId, true);
            sobj = llist.getSortListObject(key);
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
        } finally {
        }
    }

    public void doGetSize(InputStream in, ByteArrayOutputStream out) throws Exception {
        ISortList llist = null;
        try {
            String listId = CommonTools.readString(in);
            long size = 0;
            llist = factory.getList(listId, true);
            size = llist.getSize();
            CommonTools.writeString(out, "ok");
            CommonTools.writeString(out, "" + size);
        } catch (Exception e) {
            CommonTools.writeString(out, "error:" + e.getMessage());
            e.printStackTrace();
        } finally {
        }
    }

    public void doReorder(InputStream in, ByteArrayOutputStream out) throws Exception {
        ISortList llist = null;
        try {
            String listId = CommonTools.readString(in);
            Tools.checkNameLength(listId);
            String old_key = CommonTools.readString(in);
            if (StringUtils.containsAny(old_key, invalidchars)) {
                throw new Exception(listId + " sortObj key can't include [, ; : \\r \\n]");
            }
            String old_objid = CommonTools.readString(in);
            if (StringUtils.containsAny(old_objid, invalidchars)) {
                throw new Exception(listId + " sortObj objid can't include [, ; : \\r \\n]");
            }
            String new_key = CommonTools.readString(in);
            if (StringUtils.containsAny(new_key, invalidchars)) {
                throw new Exception(listId + " sortObj key can't include [, ; : \\r \\n]");
            }
            String new_objid = CommonTools.readString(in);
            if (StringUtils.containsAny(new_objid, invalidchars)) {
                throw new Exception(listId + " sortObj objid can't include [, ; : \\r \\n]");
            }
            Tools.checkKeyLength(new_key);
            Tools.checkKeyLength(new_objid);
            llist = factory.getList(listId, true);
            llist.reorder(new SortListObject(old_key, old_objid), new SortListObject(new_key, new_objid));
            CommonTools.writeString(out, "ok");
        } catch (Exception e) {
            CommonTools.writeString(out, "error:" + e.getMessage());
            e.printStackTrace();
        } finally {
        }
    }

    public ByteArrayOutputStream doSyncDataItems(InputStream in) throws Exception {
        while (true) {
            long version = 0L;
            String line = null;
            try {
                version = CommonTools.readLong(in);
                byte[] data = CommonTools.readBytes(in);
                line = new String(data, "UTF-8");
            } catch (Exception e) {
                break;
            }
            try {
                writes++;
                ((SortBandListFactory) getFactory()).writeLogAndCache(version, line);
            } catch (Exception e) {
                throw e;
            }
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "ok");
        return out;
    }

    public ByteArrayOutputStream doPullDataItems(InputStream in) throws Exception {
        while (true) {
            long min = 0L;
            long max = 0L;
            try {
                min = CommonTools.readLong(in);
                max = CommonTools.readLong(in);
                if (min < 1 || max < min) {
                    throw new Exception("doPullDataItems min or max error ... ");
                }
            } catch (Exception e) {
                break;
            }
            try {
                return ((SortBandListFactory) getFactory()).pullDataItems(min, max);
            } catch (Exception e) {
                throw e;
            }
        }
        throw new Exception("doPullDataItems error ...... ");
    }

    public ByteArrayOutputStream doCommand(InputStream in) throws Exception {
        try {
            String cmd = CommonTools.readString(in);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            if (cmd.equals("version")) {
                long ver = ((SortBandListFactory) getFactory()).verLogger.getVersion();
                CommonTools.writeString(out, String.valueOf(ver));
                return out;
            }
            return null;
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public void stop() {
        try {
            ((SortBandListFactory) factory).stop();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "can not stop listServer.", e);
        }
    }
}


