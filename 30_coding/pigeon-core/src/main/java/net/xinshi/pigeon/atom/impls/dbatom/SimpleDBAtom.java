package net.xinshi.pigeon.atom.impls.dbatom;

import net.xinshi.pigeon.atom.IIntegerAtom;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: zxy
 * Date: 2009-12-6
 * Time: 22:08:10
 * To change this template use File | Settings | File Templates.
 */
public class SimpleDBAtom implements IIntegerAtom {
    DataSource ds;
    String tableName;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public DataSource getDs() {
        return ds;
    }

    public void setDs(DataSource ds) {
        this.ds = ds;
    }

    synchronized public boolean createAndSet(String name, Integer initValue) throws Exception {
        String sql = "select count(1) from " + tableName + " where name=?";
        Connection conn = null;
        try {
            conn = ds.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, name);
            ResultSet rs = pstmt.executeQuery();
            rs.next();
            int count = rs.getInt(1);
            if (count > 0) {
                rs.close();
                pstmt.close();
                return false;
            }

            sql = String.format("insert into %s (name, value)values(?,?)", tableName);
            sql = String.format(sql, tableName);
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, name);
            pstmt.setInt(2, initValue);
            pstmt.execute();
            pstmt.close();
            return true;

        } finally {
            if (conn != null && conn.isClosed() == false) {
                conn.close();
            }
        }

    }

    public long greaterAndIncReturnLong(String name, int testValue, int incValue) throws Exception {
        throw new Exception("Not implemented!");
    }

    public long lessAndIncReturnLong(String name, int testValue, int incValue) throws Exception {
        throw new Exception("Not implemented!");
    }

    @Override
    public boolean greaterAndInc(String name, int testValue, int incValue) throws Exception {
        long rl = greaterAndIncReturnLong(name, testValue, incValue);
        return true;
    }

    @Override
    public boolean lessAndInc(String name, int testValue, int incValue) throws Exception {
        long rl = lessAndIncReturnLong(name, testValue, incValue);
        return true;
    }

    public Long get(String name) throws Exception {
        String sql = String.format("select value from %s where name=?", tableName);

        Connection conn = null;
        try {
            conn = ds.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, name);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getLong(1);
            } else {
                throw new Exception("atom '" + name + "' not exists.");
            }

        } finally {
            if (conn != null && conn.isClosed() == false) {
                conn.close();
            }
        }
    }

    @Override
    public List<Long> getAtoms(List<String> atomIds) throws Exception {
        throw new Exception("not implemented.");
    }

    public void init() throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void set_state_word(int state_word) throws Exception {

    }
}
