package net.xinshi.pigeon.client.minaclient;

import java.util.UUID;

/**
 * Created by IntelliJ IDEA.
 * User: zhengxiangyang
 * Date: 11-11-1
 * Time: 下午12:06
 * To change this template use File | Settings | File Templates.
 */
public class IdentityKeygen {
    private static ThreadLocal context = new ThreadLocal();

    public static String get() {
        String uuid = (String) context.get();
        if (uuid == null) {
            uuid = UUID.randomUUID().toString();
            String threadName = Thread.currentThread().getName();
            uuid = threadName + "@" + uuid;
            context.set(uuid);
        }
        return uuid;
//        return Thread.currentThread().getName();
    }
}
