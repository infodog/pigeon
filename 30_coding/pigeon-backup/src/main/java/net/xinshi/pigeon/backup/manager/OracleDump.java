package net.xinshi.pigeon.backup.manager;

import net.xinshi.pigeon.backup.util.BackupTools;
import net.xinshi.pigeon.netty.common.Constants;

import javax.sql.DataSource;
import java.io.FileOutputStream;
import java.sql.*;

/**
 * Created with IntelliJ IDEA.
 * User: WPF
 * Date: 13-10-10
 * Time: 上午11:37
 * To change this template use File | Settings | File Templates.
 */

public class OracleDump {

    static String[] table = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F"};

    static String bytes2hex(byte[] array) {
        StringBuilder sb = new StringBuilder(array.length * 2);
        for (int i = 0; i < array.length; i++) {
            sb.append(table[(array[i] >> 4) & 0xF]);
            sb.append(table[array[i] & 0xF]);
        }
        return sb.toString();
    }

    public static void dump_object(DataSource ds, String table, String key, String filename) throws Exception {
        long st = System.currentTimeMillis();
        long pos = 1;
        long size = 1000;
        if (key == null) {
            key = "%";
        } else {
            if (key.indexOf('%') >= 0) {
                throw new Exception("OracleDump dump_object key has %");
            }
            key += ":::%";
        }
        String statement = "SELECT * FROM (SELECT T.*, ROWNUM AS R__ FROM (SELECT * FROM %s where name like '%s' ORDER BY NAME) T WHERE ROWNUM < %d) WHERE R__ >= %d";
        Connection conn = null;
        FileOutputStream fis = new FileOutputStream(filename);
        try {
            conn = ds.getConnection();
            while (true) {
                String sql = String.format(statement, table, key, pos + size, pos);
                System.out.println(sql);
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery();
                long count = 0;
                while (rs.next()) {
                    ++count;
                    String name = rs.getString("NAME");
                    Blob content = rs.getBlob("CONTENT");
                    int isCompressed = rs.getInt("ISCOMPRESSED");
                    int isString = rs.getInt("ISSTRING");
                    if (!key.equals("%")) {
                        name = name.substring(key.length() - 1);
                    }
                    String record = String.format("%s %s %d %d\r\n", bytes2hex(name.getBytes()), bytes2hex(content.getBytes(1, (int) content.length())), isCompressed, isString);
                    fis.write(record.getBytes());
                }
                rs.close();
                stmt.close();
                if (count != size) {
                    break;
                }
                pos += size;
            }
        } finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
            if (fis != null) {
                fis.close();
            }
        }
        long et = System.currentTimeMillis();
        System.out.println("OracleDump dump_object finished time ms = " + (et - st));
    }

    public static void dump_list(DataSource ds, String table, String key, String filename) throws Exception {
        long st = System.currentTimeMillis();
        long pos = 1;
        long size = 1000;
        if (key == null) {
            key = "%";
        } else {
            if (key.indexOf('%') >= 0) {
                throw new Exception("OracleDump dump_list key has %");
            }
            key += ":::%";
        }
        String statement = "SELECT * FROM (SELECT T.*, ROWNUM AS R__ FROM (SELECT * FROM %s where listName like '%s' and isMeta=0 ORDER BY listName) T WHERE ROWNUM < %d) WHERE R__ >= %d";
        Connection conn = null;
        FileOutputStream fis = new FileOutputStream(filename);
        try {
            conn = ds.getConnection();
            while (true) {
                String sql = String.format(statement, table, key, pos + size, pos);
                System.out.println(sql);
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery();
                long count = 0;
                while (rs.next()) {
                    ++count;
                    String listName = rs.getString("listName");
                    Clob value = rs.getClob("value");
                    if (!key.equals("%")) {
                        listName = listName.substring(key.length() - 1);
                    }
                    String record = String.format("%s %s\r\n", bytes2hex(listName.getBytes()), bytes2hex(value.getSubString(1, (int) listName.length()).getBytes()));
                    fis.write(record.getBytes());
                }
                rs.close();
                stmt.close();
                if (count != size) {
                    break;
                }
                pos += size;
            }
        } finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
            if (fis != null) {
                fis.close();
            }
        }
        long et = System.currentTimeMillis();
        System.out.println("OracleDump dump_list finished time ms = " + (et - st));
    }

    public static void dump_atom(DataSource ds, String table, String key, String filename) throws Exception {
        long st = System.currentTimeMillis();
        long pos = 1;
        long size = 1000;
        if (key == null) {
            key = "%";
        } else {
            if (key.indexOf('%') >= 0) {
                throw new Exception("OracleDump dump_atom key has %");
            }
            key += ":::%";
        }
        String statement = "SELECT * FROM (SELECT T.*, ROWNUM AS R__ FROM (SELECT * FROM %s where name like '%s' ORDER BY name) T WHERE ROWNUM < %d) WHERE R__ >= %d";
        Connection conn = null;
        FileOutputStream fis = new FileOutputStream(filename);
        try {
            conn = ds.getConnection();
            while (true) {
                String sql = String.format(statement, table, key, pos + size, pos);
                System.out.println(sql);
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery();
                long count = 0;
                while (rs.next()) {
                    ++count;
                    String name = rs.getString("name");
                    int value = rs.getInt("value");
                    if (!key.equals("%")) {
                        name = name.substring(key.length() - 1);
                    }
                    String record = String.format("%s %d\r\n", bytes2hex(name.getBytes()), value);
                    fis.write(record.getBytes());
                }
                rs.close();
                stmt.close();
                if (count != size) {
                    break;
                }
                pos += size;
            }
        } finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
            if (fis != null) {
                fis.close();
            }
        }
        long et = System.currentTimeMillis();
        System.out.println("OracleDump dump_atom finished time ms = " + (et - st));
    }

    public static void dump_ids(DataSource ds, String table, String key, String filename) throws Exception {
        long st = System.currentTimeMillis();
        long pos = 1;
        long size = 1000;
        if (key == null) {
            key = "%";
        } else {
            if (key.indexOf('%') >= 0) {
                throw new Exception("OracleDump dump_ids key has %");
            }
            key += ":::%";
        }
        String statement = "SELECT * FROM (SELECT T.*, ROWNUM AS R__ FROM (SELECT * FROM %s where TableName like '%s' ORDER BY TableName) T WHERE ROWNUM < %d) WHERE R__ >= %d";
        Connection conn = null;
        FileOutputStream fis = new FileOutputStream(filename);
        try {
            conn = ds.getConnection();
            while (true) {
                String sql = String.format(statement, table, key, pos + size, pos);
                System.out.println(sql);
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery();
                long count = 0;
                while (rs.next()) {
                    ++count;
                    String TableName = rs.getString("TableName");
                    int NextValue = rs.getInt("NextValue");
                    if (!key.equals("%")) {
                        TableName = TableName.substring(key.length() - 1);
                    }
                    String record = String.format("%s %s\r\n", bytes2hex(TableName.getBytes()), NextValue);
                    fis.write(record.getBytes());
                }
                rs.close();
                stmt.close();
                if (count != size) {
                    break;
                }
                pos += size;
            }
        } finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
            if (fis != null) {
                fis.close();
            }
        }
        long et = System.currentTimeMillis();
        System.out.println("OracleDump dump_ids finished time ms = " + (et - st));
    }

    public static String dump(short type, String key, String table, DataSource ds) throws Exception {
        String path = BackupTools.getBackupPath();
        String file = DumpManager.buildLogFile(type, key, path);
        if (type == Constants.FLEXOBJECT_TYPE) {
            dump_object(ds, table, key, file);
        } else if (type == Constants.LIST_TYPE) {
            dump_list(ds, table, key, file);
        } else if (type == Constants.ATOM_TYPE) {
            dump_atom(ds, table, key, file);
        } else if (type == Constants.ID_TYPE) {
            dump_ids(ds, table, key, file);
        }
        return file.substring(path.length() + 1);
    }

}
