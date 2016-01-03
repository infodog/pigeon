package net.xinshi.pigeon.test;

import net.xinshi.pigeon.adapter.impl.MemSortList;
import net.xinshi.pigeon.list.SortListObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: WPF
 * Date: 13-11-12
 * Time: 下午5:45
 * To change this template use File | Settings | File Templates.
 */
public class TestDevel {


    public static int findNthIndexOf(String str, String needle, int occurence) {
        int pos = -1;
        if (occurence > 0) {
            while ((pos = str.indexOf(needle, pos)) >= 0 && --occurence > 0) {
                pos += needle.length();
            }
        }
        return pos;
    }

    public static void main(String[] args) {
        final MarkedQueue mq = new MarkedQueue();

        for (int i = 0; i < 100; i++) {
            (new Thread() {
                public void run() {
                    try {
                        while (!isInterrupted()) {
                            TaskMessage tm = mq.take();
                            System.out.println(tm.getSaasName() + " " + Thread.currentThread().getId());
                            synchronized (mq.getMutex()) {
                                mq.consume(tm);
                            }
                        }
                    } catch (Exception e) {

                    }
                }
            }).start();
        }

        try {
            int i = 0;
            Thread.sleep(1000 * 5);
            TaskMessage tm = new TaskMessage("111", "222", null, null);
            mq.offer(tm);
            Thread.sleep(1000 * 1);
            mq.offer(tm);
            Thread.sleep(1000 * 1);
            mq.offer(tm);
            Thread.sleep(1000 * 1);
            mq.offer(tm);
            Thread.sleep(1000 * 1);
            mq.offer(tm);
        } catch (Exception e) {
        }

        Set<TaskMessage> st = new TreeSet<TaskMessage>();
        BlockingQueue<TaskMessage> blockingQueue = new LinkedBlockingQueue<TaskMessage>();

        TaskMessage tm = new TaskMessage("111", "222", null, null);

        blockingQueue.add(tm);

        st.add(tm);

        tm = new TaskMessage("1110", "2220", null, null);

        blockingQueue.add(tm);

        tm = new TaskMessage("111", "222", null, null);

        blockingQueue.add(tm);

        tm = new TaskMessage("111", "222", null, null);

        boolean f = blockingQueue.contains(tm);

        f = st.add(tm);

        int i = 0;

        System.out.println("main over");

    }

    public static void main00(String[] args) {
        String x = "aaa".substring(0, 0);
        BlockingQueue<Runnable> blockingQueue = new LinkedBlockingQueue<Runnable>();
        BlockingQueue queue = new LinkedBlockingQueue();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(3, 20, 30, TimeUnit.MINUTES, queue);
        for (int i = 0; i < 20; i++) {
            executor.execute(new Runnable() {
                public void run() {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println(String.format("thread %d finished", this.hashCode()));
                }
            });
            int j = 0;
        }
        for (int i = 0; i < 100; i++) {
            try {
                int s = queue.size();
                System.out.println("queue size = " + s);
                if (s == 0) {
                    break;
                }
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        executor.shutdown();
    }


    public static void mainx(String argv[]) {
        try {

            boolean x = "aaaaaaaa".matches("aa2");

            int pos = findNthIndexOf("aaaaaaaaa", "aaa", 3);

            MemSortList memSortList = new MemSortList();
            SortListObject obj = new SortListObject();
            obj.setKey("5");
            obj.setObjid("5");
            memSortList.add(obj);
            obj.setKey("1");
            obj.setObjid("1");
            memSortList.add(obj);
            obj.setKey("2");
            obj.setObjid("2");
            memSortList.add(obj);
            obj.setKey("3");
            obj.setObjid("3");
            memSortList.add(obj);
            obj.setKey("6");
            obj.setObjid("6");
            memSortList.add(obj);
            obj.setKey("4");
            obj.setObjid("4");
            memSortList.add(obj);
            List<SortListObject> listObjects = new ArrayList<SortListObject>();
            obj.setKey("0");
            obj.setObjid("0");
            listObjects.add(obj);
            obj = new SortListObject("7", "7");
            listObjects.add(obj);
            obj = new SortListObject("9", "9");
            listObjects.add(obj);
            obj = new SortListObject("8", "8");
            listObjects.add(obj);
            obj = new SortListObject("00", "00");
            listObjects.add(obj);
            obj = new SortListObject("99", "99");
            listObjects.add(obj);
            memSortList.add(listObjects);
            listObjects = memSortList.getRange(-1, 5);
            listObjects = memSortList.getRange(-1, 100);
            listObjects = memSortList.getRange(10, 5);
            listObjects = memSortList.getRange(100, 5);
            listObjects = memSortList.getRange(0, -1);
            listObjects = memSortList.getRange(11, 2);
            listObjects = memSortList.getRange(11, -1);
            obj = new SortListObject("00", "00");
            memSortList.delete(obj);
            obj = new SortListObject("99", "99");
            memSortList.delete(obj);
            obj = new SortListObject("0", "0");
            memSortList.delete(obj);
            obj = new SortListObject("7", "7");
            memSortList.delete(obj);
            obj = new SortListObject("3", "3");
            SortListObject obj2 = new SortListObject("44", "44");
            memSortList.reorder(obj, obj2);
            obj = new SortListObject("4", "4");
            boolean b = memSortList.isExists("40", "4");
            long i = 0;
            i = memSortList.getLessOrEqualPos(obj);
            obj = new SortListObject("55", "55");
            List<SortListObject> ls;
            ls = memSortList.getHigherOrEqual(obj, 100);
            obj = new SortListObject("9", "9");
            i = memSortList.getLessOrEqualPos(obj);
            obj = new SortListObject("99", "99");
            i = memSortList.getLessOrEqualPos(obj);
            obj = new SortListObject("0", "0");
            i = memSortList.getLessOrEqualPos(obj);
            obj = new SortListObject("5", "5");
            i = memSortList.getLessOrEqualPos(obj);

            obj = memSortList.getSortListObject("5");
            i = memSortList.getSize();
            ++i;
        } catch (Exception e) {
        }
    }
}
