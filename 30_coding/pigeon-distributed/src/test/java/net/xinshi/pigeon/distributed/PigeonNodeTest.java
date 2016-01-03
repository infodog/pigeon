package net.xinshi.pigeon.distributed;

import net.xinshi.pigeon.distributed.client.ClientNode;
import net.xinshi.pigeon.distributed.client.NodesDispatcher;
import net.xinshi.pigeon.netty.client.Client;
import net.xinshi.pigeon.netty.common.PigeonFuture;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.concurrent.CountDownLatch;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-4
 * Time: 上午9:31
 * To change this template use File | Settings | File Templates.
 */

public class PigeonNodeTest {

    static String host = "localhost";
    static short port = Constants.CONTROL_SERVER_PORT;

    static final Client ch = new Client(host, port, 1);

    static public class SubThread extends Thread {
        private CountDownLatch runningThreadNum;

        public SubThread(CountDownLatch runningThreadNum) {
            this.runningThreadNum = runningThreadNum;
        }

        @Override
        public void run() {
            int err = 0;
            if (err > 0) {
                System.out.println("over ............ err: " + err);
            }
            runningThreadNum.countDown();
        }
    }

    public static void main(String[] args) throws Exception {
        {
            // NodesDispatcher nd = new NodesDispatcher("localhost", 3309);
            NodesDispatcher nd = new NodesDispatcher("pigeon-distributed\\src\\main\\resources\\pigeonnodes.conf");
            boolean rc = nd.init();
            ClientNode cn = nd.dispatchToNode("list", "hello");

        }
        boolean rc = ch.init();
        if (!rc) {
            return;
        }
        {
            JSONObject jo = new JSONObject();
            jo.put("type", "flexobject");
            jo.put("name", "flexobject10");
            jo.put("finger", "pigeon");
            jo.put("baseurl", "http://localhost");
            jo.put("host", "localhost");
            jo.put("port", "1234");
            byte[] tmp = jo.toString().getBytes();
            {
                PigeonFuture pf = ch.send((short) 0x0A, tmp);
                if (pf == null || !pf.waitme(5000)) {
                    System.out.println("ch send error");
                    return;
                }
                String info = new String(pf.getData(), 10, pf.getData().length - 10, "UTF-8");
                System.out.println(info);
                JSONObject jor = new JSONObject(info);
                String result = jor.optString("result");
                JSONArray jar = jor.getJSONArray("members");
            }
        }
        {
            JSONObject jo = new JSONObject();
            jo.put("type", "flexobject");
            jo.put("name", "flexobject10");
            jo.put("finger", "pigeon");
            jo.put("baseurl", "http://localhost");
            jo.put("host", "localhost");
            jo.put("port", "1234");
            jo.put("data_version", "123");
            byte[] tmp = jo.toString().getBytes();
            {
                PigeonFuture pf = ch.send((short) 0x0A, tmp);
                if (pf == null || !pf.waitme(5000)) {
                    System.out.println("ch send error");
                    return;
                }
                String info = new String(pf.getData(), 10, pf.getData().length - 10, "UTF-8");
                System.out.println(info);
                JSONObject jor = new JSONObject(info);
                String result = jor.optString("result");
                JSONArray jar = jor.getJSONArray("members");
            }
        }
        {
            {
                byte[] tmp = "hello".getBytes();
                PigeonFuture pf = ch.send((short) 0x0C, tmp);
                if (pf == null || !pf.waitme(5000)) {
                    System.out.println("ch send error");
                    return;
                }
                String info = new String(pf.getData(), 10, pf.getData().length - 10, "UTF-8");
                System.out.println(info);
                JSONObject jor = new JSONObject(info);
            }
        }
        long startTime = System.currentTimeMillis();
        int max = 10;
        CountDownLatch runningThreadNum = new CountDownLatch(max);
        for (int i = 0; i < max; i++) {
            new SubThread(runningThreadNum).start();
        }
        System.out.println("create threads over!");
        long startTime2 = System.currentTimeMillis();
        runningThreadNum.await();
        long endTime = System.currentTimeMillis();
        System.out.println("main total time : " + (endTime - startTime) + " ms");
        System.out.println("main total time : " + (endTime - startTime2) + " ms");
    }
}
