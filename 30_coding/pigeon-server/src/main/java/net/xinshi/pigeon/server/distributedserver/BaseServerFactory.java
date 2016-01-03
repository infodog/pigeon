package net.xinshi.pigeon.server.distributedserver;

import net.xinshi.pigeon.distributed.bean.ServerConfig;
import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;

import javax.sql.DataSource;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午12:08
 * To change this template use File | Settings | File Templates.
 */

public class BaseServerFactory {

    DataSource ds;
    DataSourceTransactionManager txManager;
    ServerConfig sc;

    public BaseServerFactory(ServerConfig sc) {
        this.sc = sc;
    }

    public DataSource getDs() {
        return ds;
    }

    public void setDs(DataSource ds) {
        this.ds = ds;
    }

    public DataSourceTransactionManager getTxManager() {
        return txManager;
    }

    public void setTxManager(DataSourceTransactionManager txManager) {
        this.txManager = txManager;
    }

    public ServerConfig getSc() {
        return sc;
    }

    public void setSc(ServerConfig sc) {
        this.sc = sc;
    }

    protected DataSource createDs() throws Exception {
        BasicDataSource lowlevelds = new BasicDataSource();
        String driverClass = sc.getDriverClass();
        if (driverClass == null) {
            driverClass = "org.gjt.mm.mysql.Driver";
        }
        System.out.println("driverClass is " + driverClass);
        lowlevelds.setDriverClassName(driverClass);
        lowlevelds.setUrl(sc.getDbUrl());
        lowlevelds.setUsername(sc.getDbUserName());
        lowlevelds.setPassword(sc.getDbPassword());
        lowlevelds.addConnectionProperty("socketTimeout", "1200000");
        lowlevelds.setPoolPreparedStatements(true);
        lowlevelds.setMaxActive(30);
        lowlevelds.setDefaultAutoCommit(false);
        lowlevelds.setValidationQuery("select count(*) from t_testwhileidle");
        lowlevelds.setTestOnBorrow(true);
        lowlevelds.setTestOnReturn(true);
        lowlevelds.setTestWhileIdle(true);
        lowlevelds.setTimeBetweenEvictionRunsMillis(1200000);
        ds = new TransactionAwareDataSourceProxy(lowlevelds);
        txManager = new DataSourceTransactionManager();
        txManager.setDataSource(this.ds);
        return ds;
    }

}

