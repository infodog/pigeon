package net.xinshi.pigeon.test;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created with IntelliJ IDEA.
 * User: WPF
 * Date: 13-11-15
 * Time: 下午3:40
 * To change this template use File | Settings | File Templates.
 */
public class MarkedQueue {
    Object mutex = new Object();
    Set<TaskMessage> set = new TreeSet<TaskMessage>();
    BlockingQueue<TaskMessage> queue = new LinkedBlockingDeque<TaskMessage>();

    public Object getMutex() {
        return mutex;
    }

    public void offer(TaskMessage tm) throws Exception {
        synchronized (mutex) {
            if (!queue.contains(tm) && !set.contains(tm)) {
                queue.offer(tm);
                synchronized (queue) {
                    queue.notifyAll();
                }
            }
        }
    }

    public TaskMessage take() throws Exception {
        TaskMessage tm = null;
        while (true) {
            synchronized (mutex) {
                if (queue.size() > 0) {
                    tm = queue.take();
                    set.add(tm);
                    break;
                }
            }
            synchronized (queue) {
                queue.wait();
            }
        }
        return tm;
    }

    public void consume(TaskMessage tm) throws Exception {
        synchronized (mutex) {
            set.remove(tm);
        }
    }
}
