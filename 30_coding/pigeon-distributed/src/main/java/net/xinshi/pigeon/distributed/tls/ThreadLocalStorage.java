package net.xinshi.pigeon.distributed.tls;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-6-8
 * Time: 上午9:35
 * To change this template use File | Settings | File Templates.
 */

public class ThreadLocalStorage {

    Map<Thread, Map> tlsMap = Collections.synchronizedMap(new HashMap<Thread, Map>());

    public boolean isOpen() {
        return tlsMap.get(Thread.currentThread()) == null ? false : true;
    }

    public void setOpen(boolean open) {
        if (open) {
            newMap();
        } else {
            deleteMap();
        }
    }

    synchronized private void newMap() {
        Map map = tlsMap.get(Thread.currentThread());
        if (map == null) {
            map = new HashMap();
            tlsMap.put(Thread.currentThread(), map);
        }
    }

    synchronized private void deleteMap() {
        tlsMap.remove(Thread.currentThread());
    }

    public void putMap(Object key, Object val) {
        Map map = tlsMap.get(Thread.currentThread());
        if (map != null) {
            map.put(key, val);
        }
    }

    public Object getMap(Object key) {
        Map map = tlsMap.get(Thread.currentThread());
        if (map != null) {
            return map.get(key);
        }
        return null;
    }

}

