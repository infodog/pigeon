package net.xinshi.pigeon.distributed.bean;

import net.xinshi.pigeon.distributed.util.JSONConvert;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-3
 * Time: 下午5:57
 * To change this template use File | Settings | File Templates.
 */

public class HashRange implements Comparable<HashRange> {

    String range;
    int leftBoundary;
    int rightBoundary;
    List<PigeonNode> members;

    public List<PigeonNode> getMembers() {
        return members;
    }

    public void setMembers(List<PigeonNode> members) {
        this.members = members;
    }

    public String getRange() {
        return range;
    }

    public void setRange(String range) {
        this.range = range;
    }

    public int getLeftBoundary() {
        return leftBoundary;
    }

    public void setLeftBoundary(int leftBoundary) {
        this.leftBoundary = leftBoundary;
    }

    public int getRightBoundary() {
        return rightBoundary;
    }

    public void setRightBoundary(int rightBoundary) {
        this.rightBoundary = rightBoundary;
    }

    public int compareTo(HashRange o) {
        if (this.leftBoundary == o.leftBoundary) {
            return this.rightBoundary - o.rightBoundary;
        }
        return this.leftBoundary - o.leftBoundary;
    }

    public HashRange(String range) {
        this.range = range;
        String[] parts = range.split("~");
        if (parts.length != 2) {
            return;
        }
        leftBoundary = Integer.parseInt(parts[0]);
        rightBoundary = Integer.parseInt(parts[1]);
        members = new ArrayList<PigeonNode>();
    }

    public synchronized PigeonNode addPigeonNode(PigeonNode pn) throws Exception {
        for (PigeonNode p : members) {
            if (p.getName().compareToIgnoreCase(pn.getName()) == 0) {
                // throw new Exception("the name node exists : " + pn.getName());
            }
        }
        pn.setRange(range);
        members.add(pn);
        Collections.sort(members);
        return pn;
    }

    public JSONObject toJSONObject() throws Exception {
        JSONObject jo = new JSONObject();
        jo.put("range", range);
        JSONArray ja = JSONConvert.HashRange2JSONArray(this);
        jo.put("members", ja);
        return jo;
    }

}

