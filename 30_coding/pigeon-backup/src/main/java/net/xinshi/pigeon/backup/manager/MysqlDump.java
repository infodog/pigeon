package net.xinshi.pigeon.backup.manager;

import net.xinshi.pigeon.backup.util.BackupTools;
import net.xinshi.pigeon.netty.common.Constants;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-9-4
 * Time: 上午10:22
 * To change this template use File | Settings | File Templates.
 */

public class MysqlDump {

    public static String buildSQL(short type, String key, String table, String file) throws Exception {
        String sql = null;
        int pos = 1;

        if (key == null) {
            key = "%";
        } else {
            if (key.indexOf('%') >= 0) {
                throw new Exception("MysqlDump buildSQL key has %");
            }
            key += ":::%";
            pos = key.length();
        }
        switch (type) {
            case Constants.FLEXOBJECT_TYPE:
                sql = String.format("select hex(substring(name, %d)), hex(content), isCompressed, isString from %s where name like '%s' into outfile '%s'", pos, table, key, file);
                break;
            case Constants.LIST_TYPE:
                sql = String.format("select hex(substring(listName, %d)), hex(value) from %s where listName like '%s' and isMeta=0 into outfile '%s'", pos, table, key, file);
                break;
            case Constants.ATOM_TYPE:
                sql = String.format("select hex(substring(name, %d)), value from %s where name like '%s' into outfile '%s'", pos, table, key, file);
                break;
            case Constants.ID_TYPE:
                sql = String.format("select hex(TableName), NextValue from %s into outfile '%s'", table, file);
                break;
        }
        sql = sql.replace("\\", "\\\\");

        return sql;
    }

    public static void dump(DataSource ds, String sql) throws Exception {
        long st = System.currentTimeMillis();
        Connection conn = null;
        try {
            conn = ds.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            rs.close();
            stmt.close();
        } finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
        long et = System.currentTimeMillis();
        System.out.println("MysqlDump dump finished time ms = " + (et - st));
    }

    public static String dump(short type, String key, String table, DataSource ds) throws Exception {
        long st = System.currentTimeMillis();
        Connection conn = null;
        String path = BackupTools.getBackupPath();
        String file = DumpManager.buildLogFile(type, key, path);
        String sql = MysqlDump.buildSQL(type, key, table, file);
        try {
            conn = ds.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            rs.close();
            stmt.close();
        } finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
        long et = System.currentTimeMillis();
        System.out.println("MysqlDump dump finished time ms = " + (et - st));
        return file.substring(path.length() + 1);
    }

}


