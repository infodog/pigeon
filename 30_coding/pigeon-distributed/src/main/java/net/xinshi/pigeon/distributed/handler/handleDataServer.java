package net.xinshi.pigeon.distributed.handler;

import net.xinshi.pigeon.distributed.Constants;
import net.xinshi.pigeon.distributed.bean.HashRange;
import net.xinshi.pigeon.distributed.bean.PigeonNode;
import net.xinshi.pigeon.distributed.manager.PigeonNodesManager;
import net.xinshi.pigeon.distributed.util.JSONConvert;
import net.xinshi.pigeon.util.CommonTools;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-3
 * Time: 下午10:38
 * To change this template use File | Settings | File Templates.
 */

public class handleDataServer {

    PigeonNodesManager nodesManager;

    public handleDataServer(PigeonNodesManager nodesManager) {
        this.nodesManager = nodesManager;
    }

    public JSONObject loginAction(JSONObject jobj) throws Exception {
        String type = jobj.optString("type");
        PigeonNode pn = JSONConvert.JSONObject2PigeonNode(type, jobj);
        PigeonNode py = nodesManager.findPigeonNode(pn);
        if (py != null) {
            py.assignment(pn);
            JSONObject json = new JSONObject();
            json.put("result", "ok");
            HashRange hr = nodesManager.findHashRange(py);
            json.put("members", JSONConvert.HashRange2JSONArray(hr));
            System.out.println(json.toString());
            return json;
        }
        return null;
    }

    public JSONObject aliveAction(JSONObject jobj) throws Exception {
        String type = jobj.optString("type");
        PigeonNode pn = JSONConvert.JSONObject2PigeonNode(type, jobj);
        PigeonNode py = nodesManager.findPigeonNode(pn);
        if (py != null) {
            py.setData_version(pn.getData_version());
            py.setLastHeartbeat(System.currentTimeMillis());
            py.setActive(true);
            JSONObject json = new JSONObject();
            json.put("result", "ok");
            HashRange hr = nodesManager.findHashRange(py);
            json.put("members", JSONConvert.HashRange2JSONArray(hr));
            System.out.println(json.toString());
            return json;
        }
        return null;
    }

    public JSONObject nodesAction() throws Exception {
        JSONObject json = JSONConvert.PigeonNodes2JSONObject(nodesManager.getVersion(), nodesManager.getPigeonTypes());
        System.out.println(json.toString());
        return json;
    }

    public ByteArrayOutputStream handler(byte[] buffer) throws Exception {
        ByteArrayOutputStream os = null;
        // int sq = CommonTools.bytes2intJAVA(buffer, 4);
        short flag = CommonTools.bytes2shortJAVA(buffer, 8);
        int t = (flag >> 8) & 0xFF;
        int n = flag & 0xFF;

        String info = new String(buffer, Constants.PACKET_PREFIX_LENGTH, buffer.length - Constants.PACKET_PREFIX_LENGTH, "UTF-8");

        if (t == net.xinshi.pigeon.netty.common.Constants.CONTROL_TYPE) {
            if (n == Constants.ACTION_LOGIN) {
                JSONObject result = loginAction(new JSONObject(info));
                byte[] tmp = result.toString().getBytes();
                os = new ByteArrayOutputStream(tmp.length);
                os.write(tmp);
            }
            if (n == Constants.ACTION_HEARTBEAT) {
                JSONObject result = aliveAction(new JSONObject(info));
                byte[] tmp = result.toString().getBytes();
                os = new ByteArrayOutputStream(tmp.length);
                os.write(tmp);
            }
            if (n == Constants.ACTION_NODES_INFO) {
                JSONObject result = nodesAction();
                byte[] tmp = result.toString().getBytes();
                os = new ByteArrayOutputStream(tmp.length);
                os.write(tmp);
            }
        }

        return os;
    }

}

