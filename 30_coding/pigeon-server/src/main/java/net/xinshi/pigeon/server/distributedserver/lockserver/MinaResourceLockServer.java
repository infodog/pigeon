package net.xinshi.pigeon.server.distributedserver.lockserver;

import net.xinshi.pigeon.distributed.bean.ServerConfig;
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

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午5:47
 * To change this template use File | Settings | File Templates.
 */

public class MinaResourceLockServer {

    ServerConfig sc;
    Log log = LogFactory.getLog(MinaResourceLockServer.class);
    protected static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    protected String startTime;
    IoAcceptor acceptor;

    public MinaResourceLockServer(ServerConfig sc) {
        super();
        this.sc = sc;
        this.startTime = this.dateFormat.format(new Date());
    }

    public void start() throws Exception {
        Integer PORT = Integer.valueOf(sc.getlockPort());
        NioSocketAcceptor nioSA = new NioSocketAcceptor();
        nioSA.setReuseAddress(true);
        acceptor = nioSA;
        acceptor.getFilterChain().addLast("logger", new LoggingFilter());
        acceptor.getFilterChain().addLast("codec", new ProtocolCodecFilter(new TextLineCodecFactory(Charset.forName("UTF-8"), "\n", "\n")));
        acceptor.setHandler(new MinaLockServerHandler());
        acceptor.getSessionConfig().setReadBufferSize(2048);
        acceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE, 60);
        ((DefaultSocketSessionConfig) acceptor.getSessionConfig()).setReuseAddress(true);
        acceptor.bind(new InetSocketAddress(PORT));
    }

    public void stop() {
        acceptor.unbind();
    }
}


