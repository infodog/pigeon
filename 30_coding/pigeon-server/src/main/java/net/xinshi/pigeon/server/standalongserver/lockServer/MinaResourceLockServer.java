package net.xinshi.pigeon.server.standalongserver.lockServer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.DefaultSocketSessionConfig;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: zhengxiangyang
 * Date: 11-10-30
 * Time: 上午4:29
 * To change this template use File | Settings | File Templates.
 */
public class MinaResourceLockServer {
    Log log = LogFactory.getLog(MinaResourceLockServer.class);

    protected String localPort;

    protected boolean isMaster;
    protected String nodeName;
    protected String instanceName;
    protected String type;

    protected boolean isSuspended = false;
    protected boolean synReplication = false;
    protected static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    protected String startTime;

    protected long writes = 0;
    protected long reads = 0;

    IoAcceptor acceptor;

    public MinaResourceLockServer() {
        super();

        this.startTime = this.dateFormat.format(new Date());
    }

    public void start(Map config) throws Exception {
        this.isMaster = (Boolean) config.get("master");

        nodeName = (String) config.get("nodeName");
        instanceName = (String) config.get("instanceName");
        this.type = (String) config.get("type");

        Integer PORT = (Integer) config.get("lockPort");
        NioSocketAcceptor nioSA = new NioSocketAcceptor();
        nioSA.setReuseAddress(true);
        acceptor = nioSA;

        // Add two filters : a logger and a codec
        acceptor.getFilterChain().addLast("logger", new LoggingFilter());
        acceptor.getFilterChain().addLast("codec", new ProtocolCodecFilter(new TextLineCodecFactory(Charset.forName("UTF-8"), "\n", "\n")));

        // Attach the business logic to the server
        acceptor.setHandler(new MinaLockServerHandler());

        // Configurate the buffer size and the iddle time
        acceptor.getSessionConfig().setReadBufferSize(2048);
        acceptor.getSessionConfig().setIdleTime( IdleStatus.BOTH_IDLE,60 );

        ((DefaultSocketSessionConfig)acceptor.getSessionConfig()).setReuseAddress(true);

        // And bind !
        acceptor.bind(new InetSocketAddress(PORT));

    }

    public void stop(){
        acceptor.unbind();
    }


}

