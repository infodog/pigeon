package net.xinshi.pigeon.server.standalongserver;

/**
 * Created by IntelliJ IDEA.
 * User: zhengxiangyang
 * Date: 11-10-29
 * Time: 下午3:38
 * To change this template use File | Settings | File Templates.
 */

import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.params.ConnManagerParams;
import org.apache.http.conn.params.ConnPerRouteBean;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public abstract class BaseServer implements IServer {
    protected HttpClient httpClient;

    protected String seedUrl;

    protected String localPort;

    protected boolean isMaster = false;
    protected String nodeName;
    protected String instanceName;
    protected String type;

    protected boolean isSuspended = false;

    protected static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    protected String startTime;
    protected long writes = 0;
    protected long reads = 0;
    protected boolean isWaitingSyn = false;



    protected boolean bSyncOK = true; // WPF
    protected int version = 0;        // WPF

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public void setSeedUrl(String seedUrl) {
        this.seedUrl = seedUrl;
    }

    public BaseServer() {
        HttpParams params = new BasicHttpParams();

        ConnManagerParams.setMaxTotalConnections(params, 100);
        ConnManagerParams.setMaxConnectionsPerRoute(params, new ConnPerRouteBean(200));

        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);

        SchemeRegistry schemeRegistry = new SchemeRegistry();
        schemeRegistry.register(new Scheme("http", PlainSocketFactory.getSocketFactory(), 80));

        ClientConnectionManager cm = new ThreadSafeClientConnManager(params, schemeRegistry);
        httpClient = new DefaultHttpClient(cm, params);


        this.startTime = this.dateFormat.format(new Date());

    }

    public boolean isMaster() {
        return isMaster;
    }

    public void setMaster(boolean master) {
        if (master) {
            this.isWaitingSyn = true;
        }
        isMaster = master;
    }


    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }



    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public void setLocalPort(String localPort) {
        this.localPort = localPort;
    }



    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void start() throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}

