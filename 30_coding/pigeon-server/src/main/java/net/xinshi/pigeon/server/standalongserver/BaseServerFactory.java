package net.xinshi.pigeon.server.standalongserver;

/**
 * Created by IntelliJ IDEA.
 * User: zhengxiangyang
 * Date: 11-10-30
 * Time: 上午12:55
 * To change this template use File | Settings | File Templates.
 */

import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;

import javax.sql.DataSource;
import java.util.Map;

public class BaseServerFactory {
    protected DataSource ds;
    protected DataSourceTransactionManager txManager;

    protected Map config;

    protected DataSource createDs() throws Exception {
        BasicDataSource lowlevelds = new BasicDataSource();
        String driverClass = (String) config.get("driverClass");
        if (driverClass == null) {
            driverClass = "org.gjt.mm.mysql.Driver";
        }
        lowlevelds.setUrl((String) config.get("dbUrl"));
        lowlevelds.setUsername((String) config.get("dbUserName"));
        lowlevelds.setPassword((String) config.get("dbPassword"));
        lowlevelds.addConnectionProperty("socketTimeout", "1200000");
        lowlevelds.setPoolPreparedStatements(true);
        lowlevelds.setMaxActive(10);
        lowlevelds.setDefaultAutoCommit(false);
        lowlevelds.setValidationQuery("select count(*) from t_testwhileidle");
        lowlevelds.setTestOnBorrow(true);
        lowlevelds.setTestOnReturn(true);
        lowlevelds.setTestWhileIdle(true);
        lowlevelds.setTimeBetweenEvictionRunsMillis(1200000);

        this.ds = new TransactionAwareDataSourceProxy(lowlevelds);
        txManager = new DataSourceTransactionManager();
        txManager.setDataSource(this.ds);
        return this.ds;
    }
}
