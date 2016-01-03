package net.xinshi.pigeon.importdata;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.persistence.VersionHistory;
import net.xinshi.pigeon.util.TimeTools;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-6-11
 * Time: 下午2:46
 * To change this template use File | Settings | File Templates.
 */

public class ImportIdServer {
    DataShare ds;
    IPigeonStoreEngine pigeonStoreEngine;
    public long lastVersion = -1;

    public ImportIdServer(config config, IPigeonStoreEngine pigeonStoreEngine) {
        this.pigeonStoreEngine = pigeonStoreEngine;
        ds = new DataShare(config);
    }

    public void init() throws Exception {
        ds.init();
    }

    public long ImportDataFiles() throws Exception {
        int n = 0;
        while (true) {
            VersionHistory vh = ds.fetchVersionHistory();
            if (vh == null) {
                break;
            }
            if (vh.getVersion() % 10000 == 0) {
                System.out.println(TimeTools.getNowTimeString() + " IdServer import version = " + vh.getVersion());
            }
            String line = new String(vh.getData(), "UTF-8");
            line = line.trim();
            line.replace("\n", "");
            String[] parts = line.split(":");
            String name = parts[0];
            long id = Long.valueOf(parts[1]);
            while (true) {
                try {
                    long delta = id - pigeonStoreEngine.getIdGenerator().getId(name);
                    if (delta < 0 || delta > 10000) {
                        System.out.println("id server name = " + name + ", delta = " + delta);
                    }
                    while (true) {
                        if (pigeonStoreEngine.getIdGenerator().getId(name) > id) {
                            break;
                        }
                    }
                    ++n;
                    break;
                } catch (Exception e) {
                    e.printStackTrace();
                    Thread.sleep(1000);
                }
            }
            lastVersion = vh.getVersion();
        }
        return n;
    }

}

