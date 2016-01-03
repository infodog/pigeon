package net.xinshi.pigeon.pigeonclient;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.adapter.impl.DistributedPigeonEngine;
import net.xinshi.pigeon.persistence.VersionHistory;
import net.xinshi.pigeon.saas.datasource.PigeonDataSource;

import java.io.File;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: WPF
 * Date: 13-1-28
 * Time: 下午12:03
 * To change this template use File | Settings | File Templates.
 */

public class PigeonDataSourceTest {

    public static void main(String[] args) throws Exception {
        File f = new File("pigeon-server/src/test/resources/pigeonnodes.conf");
        IPigeonStoreEngine pigeonStoreEngine = new DistributedPigeonEngine(f.getAbsolutePath());
        PigeonDataSource.setPigeon(pigeonStoreEngine);
        int count = PigeonDataSource.getDataSourceCount();
        System.out.println("count = " + count);
        long lastVersion = PigeonDataSource.getLastVersion(0);
        System.out.println("lastVersion = " + lastVersion);
        long cur = 1;
        long total = 0;
        long st = System.currentTimeMillis();
        while (true) {
            long end = cur + 10000;
            end -= end % 10000;
            List<VersionHistory> listVHs = PigeonDataSource.getVersionHistory(0, cur, end);
            total += listVHs.size();
            if (listVHs.size() != end - cur + 1) {
                break;
            }
            cur = end + 1;
        }
        long et = System.currentTimeMillis();
        System.out.println("total time : " + (et - st) + ", total = " + total);
    }

}

