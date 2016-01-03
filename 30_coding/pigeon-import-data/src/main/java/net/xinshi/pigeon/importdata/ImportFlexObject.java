package net.xinshi.pigeon.importdata;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.flexobject.FlexObjectEntry;
import net.xinshi.pigeon.persistence.VersionHistory;
import net.xinshi.pigeon.util.CommonTools;
import net.xinshi.pigeon.util.TimeTools;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-6-11
 * Time: 上午11:29
 * To change this template use File | Settings | File Templates.
 */

public class ImportFlexObject {
    DataShare ds;
    IPigeonStoreEngine pigeonStoreEngine;
    public long lastVersion = -1;

    public ImportFlexObject(config config, IPigeonStoreEngine pigeonStoreEngine) {
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
                System.out.println(TimeTools.getNowTimeString() + " FlexObject import version = " + vh.getVersion());
            }
            InputStream mis = new ByteArrayInputStream(vh.getData());
            FlexObjectEntry entry = CommonTools.readEntry(mis);
            if (entry == null) {
                throw new Exception("FlexObject CommonTools.readEntry == null exit ... ");
            }
            while (true) {
                try {
                    if (entry == FlexObjectEntry.empty) {
                        System.out.println("FlexObjectEntry.empty == " + vh.getVersion() + " version");
                    } else {
                        pigeonStoreEngine.getFlexObjectFactory().saveFlexObject(entry);
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
