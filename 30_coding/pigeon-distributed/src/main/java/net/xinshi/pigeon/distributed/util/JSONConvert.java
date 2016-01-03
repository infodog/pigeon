package net.xinshi.pigeon.distributed.util;

import net.xinshi.pigeon.distributed.bean.HashRange;
import net.xinshi.pigeon.distributed.bean.PigeonNode;
import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import sun.util.logging.resources.logging;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-4
 * Time: 上午10:02
 * To change this template use File | Settings | File Templates.
 */

public class JSONConvert {
    static Logger logger = Logger.getLogger(JSONConvert.class.getName());

    public static PigeonNode JSONObject2PigeonNode(String type, JSONObject jobj) {
        PigeonNode pn = new PigeonNode();
        pn.setName(jobj.optString("name"));
        pn.setFinger(jobj.optString("finger"));
        if (jobj.optString("role").length() > 0) {
            pn.setRole(jobj.optString("role").charAt(0));
        }
        pn.setType(type);
        pn.setBaseurl(jobj.optString("baseurl"));
        pn.setHost(jobj.optString("host"));
        pn.setPort(jobj.optInt("port"));
        pn.setLock_port(jobj.optInt("lock_port"));
        pn.setData_version(jobj.optLong("data_version"));
        return pn;
    }

    public static JSONObject PigeonNode2JSONObject(PigeonNode pn) throws Exception {
        JSONObject jo = new JSONObject();
        jo.put("name", pn.getName());
        jo.put("type", pn.getType());
        jo.put("role", String.valueOf(pn.getRole()));
        jo.put("baseurl", pn.getBaseurl());
        jo.put("host", pn.getHost());
        jo.put("port", pn.getPort());
        jo.put("data_version", pn.getData_version());
        jo.put("control_version", pn.getControl_version());
        jo.put("active", pn.isActive());
        return jo;
    }

    public static JSONArray HashRange2JSONArray(HashRange hr) throws Exception {
        JSONArray ja = new JSONArray();
        for (PigeonNode p : hr.getMembers()) {
            JSONObject jo = PigeonNode2JSONObject(p);
            ja.put(jo);
        }
        return ja;
    }

    private static HashRange putPigeonTypeRange(HashMap<String, List<HashRange>> PigeonTypes, String type, String range) {
        HashRange sh = null;
        List<HashRange> listHR = PigeonTypes.get(type);
        if (listHR == null) {
            listHR = new ArrayList<HashRange>();
            PigeonTypes.put(type, listHR);
        }
        for (HashRange h : listHR) {
            if (h.getRange().compareToIgnoreCase(range) == 0) {
                sh = h;
                break;
            }
        }
        if (sh == null) {
            sh = new HashRange(range);
            listHR.add(sh);
            Collections.sort(listHR);
        }
        return sh;
    }

    public static long readPigeonTypesVersion(String s) throws Exception {
        try {
            s = StringUtils.trim(s);
            JSONObject jo = new JSONObject(s);
            return jo.optLong("version");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0L;
    }

    public static HashMap<String, List<HashRange>> String2PigeonTypes(String s) throws Exception {
        try {
            HashMap<String, List<HashRange>> PigeonTypes = new HashMap<String, List<HashRange>>();
            s = StringUtils.trim(s);
            JSONObject jo = new JSONObject(s);
            JSONArray nodes = jo.getJSONArray("pigeon_nodes");
            for (int i = 0; i < nodes.length(); i++) {
                JSONObject jot = nodes.getJSONObject(i);
                String type = jot.optString("type");
                JSONArray jas = jot.getJSONArray("segments");
                for (int j = 0; j < jas.length(); j++) {
                    JSONObject jom = jas.optJSONObject(j);
                    String range = jom.optString("range");
                    HashRange hr = putPigeonTypeRange(PigeonTypes, type, range);
                    JSONArray jon = jom.getJSONArray("members");
                    for (int k = 0; k < jon.length(); k++) {
                        JSONObject jox = jon.getJSONObject(k);
                        PigeonNode pn = JSONConvert.JSONObject2PigeonNode(type, jox);
                        hr.addPigeonNode(pn);
                    }
                }
            }
            return PigeonTypes;
        } catch (Exception e) {
            logger.log(Level.WARNING,e.getMessage(),e);
            e.printStackTrace();
        }
        return null;
    }

    public static JSONObject PigeonNodes2JSONObject(long version, HashMap<String, List<HashRange>> PigeonTypes) throws Exception {
        JSONArray ja = new JSONArray();
        for (String type : PigeonTypes.keySet()) {
            JSONObject jo = new JSONObject();
            jo.put("type", type);
            JSONArray jh = new JSONArray();
            for (HashRange hr : PigeonTypes.get(type)) {
                JSONObject jso = hr.toJSONObject();
                jh.put(jso);
            }
            jo.put("segments", jh);
            ja.put(jo);
        }
        JSONObject joa = new JSONObject();
        joa.put("pigeon_nodes", ja);
        joa.put("version", version);
        return joa;
    }

}

