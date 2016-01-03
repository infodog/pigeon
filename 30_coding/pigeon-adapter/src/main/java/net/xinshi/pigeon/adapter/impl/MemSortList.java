package net.xinshi.pigeon.adapter.impl;

import net.xinshi.pigeon.list.ISortList;
import net.xinshi.pigeon.list.SortListObject;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: WPF
 * Date: 13-11-12
 * Time: 下午4:09
 * To change this template use File | Settings | File Templates.
 */
public class MemSortList implements ISortList {

    private List<SortListObject> list = new LinkedList<SortListObject>();

    private static class AscComparator implements Comparator<SortListObject> {
        public int compare(SortListObject o1, SortListObject o2) {
            int r = o1.getKey().compareTo(o2.getKey());
            if (r == 0) {
                int r1 = o1.getObjid().compareTo(o2.getObjid());
                return r1;
            } else {
                return r;
            }
        }
    }

    public synchronized List<SortListObject> getRange(int beginIndex, int number) throws Exception {
        if (beginIndex < 0) {
            beginIndex = 0;
        }
        if (beginIndex >= list.size()) {
            return new Vector();
        }
        if (number == 0) {
            return new Vector();
        }
        if (number == -1) {
            number = list.size();
        }
        int endIndex = beginIndex + number;
        if (endIndex > list.size()) {
            endIndex = list.size();
        }
        Vector result = new Vector();
        for (int i = beginIndex; i < endIndex; i++) {
            SortListObject obj = list.get(i);
            result.add(new SortListObject(obj.getKey(), obj.getObjid()));
        }
        return result;
    }

    public synchronized boolean delete(SortListObject sortObj) throws Exception {
        int pos = Collections.binarySearch(list, sortObj, new AscComparator());
        if (pos >= 0) {
            list.remove(pos);
            return true;
        }
        return false;
    }

    public synchronized boolean add(SortListObject sortObj) throws Exception {
        int pos = Collections.binarySearch(list, sortObj, new AscComparator());
        if (pos < 0) {
            SortListObject obj = new SortListObject(sortObj.getKey(), sortObj.getObjid());
            list.add(-pos - 1, obj);
        }
        return true;
    }

    public synchronized boolean add(List<SortListObject> listSortObj) throws Exception {
        for (SortListObject obj : listSortObj) {
            add(obj);
        }
        return true;
    }

    public synchronized boolean reorder(SortListObject oldObj, SortListObject newObj) throws Exception {
        delete(oldObj);
        add(newObj);
        return true;
    }

    public synchronized boolean isExists(String key, String objid) throws Exception {
        SortListObject sortListObject = new SortListObject();
        sortListObject.setKey(key);
        sortListObject.setObjid(objid);
        return Collections.binarySearch(list, sortListObject, new AscComparator()) >= 0;
    }

    public synchronized long getLessOrEqualPos(SortListObject obj) throws Exception {
        int pos = Collections.binarySearch(list, obj, new AscComparator());
        if (pos >= 0) {
            return pos;
        }
        return -pos - 1 - 1;
    }

    public synchronized SortListObject getSortListObject(String key) throws Exception {
        SortListObject obj = new SortListObject();
        obj.setKey(key);
        obj.setObjid("");
        int pos = Collections.binarySearch(list, obj, new AscComparator());
        if (pos >= 0) {
            obj = list.get(pos);
            return new SortListObject(obj.getKey(), obj.getObjid());
        }
        return null;
    }

    public synchronized List<SortListObject> getHigherOrEqual(SortListObject obj, int num) throws Exception {
        int pos = Collections.binarySearch(list, obj, new AscComparator());
        if (pos < 0) {
            pos = -pos - 1;
        }
        if (pos >= list.size()) {
            return new Vector<SortListObject>(0);
        }
        int end = pos + num;
        if (end > list.size()) {
            end = list.size();
        }
        Vector<SortListObject> listObjects = new Vector<SortListObject>();
        for (int i = pos; i < end; i++) {
            SortListObject listObject = list.get(i);
            listObjects.add(new SortListObject(listObject.getKey(), listObject.getObjid()));
        }
        return listObjects;
    }

    public synchronized long getSize() throws Exception {
        return list.size();
    }

}

