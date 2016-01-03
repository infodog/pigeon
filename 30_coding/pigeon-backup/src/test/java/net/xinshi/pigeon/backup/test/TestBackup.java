package net.xinshi.pigeon.backup.test;

import net.xinshi.pigeon.backup.manager.DumpManager;
import net.xinshi.pigeon.backup.manager.MysqlDump;
import net.xinshi.pigeon.backup.util.BackupTools;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-9-4
 * Time: 下午3:26
 * To change this template use File | Settings | File Templates.
 */
public class TestBackup {

    public static void main(String[] args) throws Exception {

        String key = "abc";
        String table = "t_table";
        String file = "/data/tmp/file.txt";

        String temp = BackupTools.buildTempFileName((short)3, key);

        System.out.println(BackupTools.getBackupPath());

        System.out.println(temp);

        System.out.println(DumpManager.buildLogFile((short)1, key, "/home/xxx"));

        String sql = MysqlDump.buildSQL((short) 4, key, table, file);

        System.out.println(sql);
    }

}
