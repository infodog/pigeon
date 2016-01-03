package net.xinshi.pigeon.dumpload.dumpdb;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-6-12
 * Time: 上午11:16
 * To change this template use File | Settings | File Templates.
 */

public class DumpFlexObjectV3 {
    static long count = -1;
    String tableName;
    String saveToFile;
    DataSource ds;

    public DumpFlexObjectV3(DataSource ds, String tableName, String saveToFile) {
        this.ds = ds;
        this.tableName = tableName;
        this.saveToFile = saveToFile;
    }

    public void dump() throws Exception {
        long st = System.currentTimeMillis();
        String sql = "select hex(name), hex(content), isCompressed, isString from " + tableName + " into outfile '" + saveToFile + "'";
        Connection conn = null;
        try {
            conn = ds.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            rs.close();
            stmt.close();
            File f = new File(saveToFile);
            System.out.println(saveToFile + " size = " + f.length());
        } finally {
            if (conn != null && conn.isClosed() == false) {
                conn.close();
            }
        }
        long et = System.currentTimeMillis();
        System.out.println("...... flexobject dump finished time ms = " + (et - st));
    }

}


