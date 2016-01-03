package net.xinshi.pigeon.distributed.manager;

import net.xinshi.pigeon.distributed.bean.HashRange;
import net.xinshi.pigeon.distributed.bean.PigeonNode;
import net.xinshi.pigeon.distributed.util.JSONConvert;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-3
 * Time: 下午5:55
 * To change this template use File | Settings | File Templates.
 */

public class PigeonNodesManager {

    long version = 0L;
    HashMap<String, List<HashRange>> PigeonTypes = null;

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public HashMap<String, List<HashRange>> getPigeonTypes() {
        return PigeonTypes;
    }

    public void setPigeonTypes(HashMap<String, List<HashRange>> pigeonTypes) {
        PigeonTypes = pigeonTypes;
    }

    public PigeonNode findPigeonNode(PigeonNode pn) {
        List<HashRange> listHR = PigeonTypes.get(pn.getType());
        if (listHR != null) {
            for (HashRange hr : listHR) {
                for (PigeonNode p : hr.getMembers()) {
                    if (p.getName().compareToIgnoreCase(pn.getName()) == 0 && p.getFinger().compareTo(pn.getFinger()) == 0) {
                        return p;
                    }
                }
            }
        }
        return null;
    }

    public HashRange findHashRange(PigeonNode pn) {
        List<HashRange> listHR = PigeonTypes.get(pn.getType());
        for (HashRange hr : listHR) {
            if (hr.getRange().compareToIgnoreCase(pn.getRange()) == 0) {
                return hr;
            }
        }
        return null;
    }

    public void init(String config) throws Exception {
        try {
            File f = new File(config);
            FileInputStream is = new FileInputStream(f);
            byte[] b = new byte[(int) f.length()];
            is.read(b);
            is.close();
            String s = new String(b, "UTF-8");
            s = StringUtils.trim(s);
            version = JSONConvert.readPigeonTypesVersion(s);
            PigeonTypes = JSONConvert.String2PigeonTypes(s);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

}

