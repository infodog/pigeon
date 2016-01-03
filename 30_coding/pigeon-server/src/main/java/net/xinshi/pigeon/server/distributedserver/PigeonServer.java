package net.xinshi.pigeon.server.distributedserver;

import net.xinshi.pigeon.backup.handler.DownloadHttpRequestHandler;
import net.xinshi.pigeon.netty.common.PigeonDecoder;
import net.xinshi.pigeon.netty.server.IServerHandler;
import net.xinshi.pigeon.netty.server.ServerHandler;
import net.xinshi.pigeon.server.distributedserver.atomserver.AtomServer;
import net.xinshi.pigeon.server.distributedserver.atomserver.NettyAtomServerHandler;
import net.xinshi.pigeon.server.distributedserver.control.ServerCommand;
import net.xinshi.pigeon.server.distributedserver.flexobjectserver.FlexObjectServer;
import net.xinshi.pigeon.server.distributedserver.flexobjectserver.NettyFlexObjectServerHandler;
import net.xinshi.pigeon.server.distributedserver.idserver.IdServer;
import net.xinshi.pigeon.server.distributedserver.idserver.NettyIdServerHandler;
import net.xinshi.pigeon.server.distributedserver.listserver.ListServer;
import net.xinshi.pigeon.server.distributedserver.listserver.NettyListServerHandler;
import net.xinshi.pigeon.server.distributedserver.lockserver.MinaResourceLockServer;
import net.xinshi.pigeon.server.distributedserver.lockserver.NettyLockServerHandler;
import net.xinshi.pigeon.server.distributedserver.monitor.StatusHttpRequestHandler;
import net.xinshi.pigeon.server.distributedserver.util.Tools;
import net.xinshi.pigeon.util.CommonTools;
import net.xinshi.pigeon.util.TimeTools;
import org.apache.http.ConnectionClosedException;
import org.apache.http.HttpException;
import org.apache.http.HttpServerConnection;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.DefaultHttpResponseFactory;
import org.apache.http.impl.DefaultHttpServerConnection;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.*;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.jboss.netty.channel.Channels.pipeline;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午2:24
 * To change this template use File | Settings | File Templates.
 */

public class PigeonServer implements IServerHandler {

    static Logger logger = Logger.getLogger(PigeonServer.class.getName());
    static ThreadPoolExecutor executor;
    static ArrayBlockingQueue<Runnable> workingQue;
    static AtomicInteger threadCount;
    public static ServerController controller = null;
    public static String controlFile = null;
    public static int nFixedThread = 100;
    public static String httpPort = Constants.defaultPort;

    public static AtomicLong getOperationsCount() {
        return operationsCount;
    }

    public static void setOperationsCount(AtomicLong operationsCount) {
        PigeonServer.operationsCount = operationsCount;
    }

    static AtomicLong operationsCount;

    public static AtomicInteger getThreadCount() {
        return threadCount;
    }

    public static void setThreadCount(AtomicInteger threadCount) {
        PigeonServer.threadCount = threadCount;
    }

    public static void main(String[] args) throws Exception {
        try {
            String configFile = Constants.defaultConfigFile;
            String port = Constants.defaultPort;
            String nettyPort = Constants.defaultNettyPort;
            if (args.length > 0) {
                configFile = args[0];
                if (configFile.compareTo("command") == 0 && args.length == 4) {
                    ServerCommand sc = new ServerCommand(args[1], args[2], args[3]);
                    sc.invoke_cmd();
                    System.exit(0);
                    return;
                }
            }
            if (args.length > 1) {
                port = args[1];
            }
            if (args.length > 2) {
                nettyPort = args[2];
            }
            if (args.length > 3) {
                controlFile = args[3];
            }
            if (args.length > 4) {
                nFixedThread = Integer.valueOf(args[4]);
            }
            logger.info("pigeon version : " + Constants.version);
            int listenPort = Integer.parseInt(port);
            httpPort = port;
            logger.info("listening:" + listenPort);
            logger.info("listening netty port: " + nettyPort);
            logger.info("using config file:" + configFile);
            logger.info("newFixedThread : " + nFixedThread);
            startServers(listenPort, Integer.parseInt(nettyPort), configFile);
            System.out.println(TimeTools.getNowTimeString() + " PigeonServer Started. version = " + Constants.version);
        } catch (Exception ex) {
            ex.printStackTrace();
            while (true) {
                try {
                    Thread.sleep(3000);
                    System.exit(0);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    static RequestListenerThread t;

    public static void startNettyServers(int nettyPort) throws Exception {
        ChannelFactory factory =
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool());
        ServerBootstrap bootstrap = new ServerBootstrap(factory);
        final ExecutionHandler eh = new ExecutionHandler(Executors.newFixedThreadPool(nFixedThread));
        final ServerHandler sh = new ServerHandler(new PigeonServer());
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline cpl = pipeline(new PigeonDecoder(), eh, sh);
                return cpl;
            }
        });
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.setOption("reuseAddress", true);
        bootstrap.bind(new InetSocketAddress(nettyPort));
    }

    public ChannelBuffer handler(Channel ch, byte[] buffer) {
        try {

            int sq = CommonTools.bytes2intJAVA(buffer, 4);
            short flag = CommonTools.bytes2shortJAVA(buffer, 8);
            int t = (flag >> 8) & 0xFF;
            int n = flag & 0xFF;
            ChannelBuffer out = null;
            String id = "";
            int f = t & 0xF0;
            if (f != 0) {
                t &= 0x0F;
            }

            if (controller == null && t != net.xinshi.pigeon.netty.common.Constants.CONTROL_TYPE) {
                System.out.println("starting ... cancel handler ... ");
                return null;
            }

            if (t == net.xinshi.pigeon.netty.common.Constants.FLEXOBJECT_TYPE) {
                id = "/flexobject" + n;
                Object obj = controller.getServers().get(id);
                if (obj instanceof FlexObjectServer) {
                    InputStream is = new ByteArrayInputStream(buffer, Constants.PACKET_PREFIX_LENGTH, buffer.length - Constants.PACKET_PREFIX_LENGTH);
                    ByteArrayOutputStream os = NettyFlexObjectServerHandler.handle((FlexObjectServer) obj, is, f);
                    if (os == null) {
                        return null;
                    }
                    out = Tools.buildChannelBuffer(sq, flag, os);
                }
            } else if (t == net.xinshi.pigeon.netty.common.Constants.LIST_TYPE) {
                id = "/list" + n;
                Object obj = controller.getServers().get(id);
                if (obj instanceof ListServer) {
                    InputStream is = new ByteArrayInputStream(buffer, Constants.PACKET_PREFIX_LENGTH, buffer.length - Constants.PACKET_PREFIX_LENGTH);
                    ByteArrayOutputStream os = NettyListServerHandler.handle((ListServer) obj, is, f);
                    if (os == null) {
                        return null;
                    }
                    out = Tools.buildChannelBuffer(sq, flag, os);
                }
            } else if (t == net.xinshi.pigeon.netty.common.Constants.ATOM_TYPE) {
                id = "/atom" + n;
                Object obj = controller.getServers().get(id);
                if (obj instanceof AtomServer) {
                    InputStream is = new ByteArrayInputStream(buffer, Constants.PACKET_PREFIX_LENGTH, buffer.length - Constants.PACKET_PREFIX_LENGTH);
                    ByteArrayOutputStream os = NettyAtomServerHandler.handle((AtomServer) obj, is, f);
                    if (os == null) {
                        return null;
                    }
                    out = Tools.buildChannelBuffer(sq, flag, os);
                }
            } else if (t == net.xinshi.pigeon.netty.common.Constants.ID_TYPE) {
                id = "/idserver" + n;
                Object obj = controller.getServers().get(id);
                if (obj instanceof IdServer) {
                    InputStream is = new ByteArrayInputStream(buffer, Constants.PACKET_PREFIX_LENGTH, buffer.length - Constants.PACKET_PREFIX_LENGTH);
                    ByteArrayOutputStream os = NettyIdServerHandler.handle((IdServer) obj, is, f);
                    if (os == null) {
                        return null;
                    }
                    out = Tools.buildChannelBuffer(sq, flag, os);
                }
            } else if (t == net.xinshi.pigeon.netty.common.Constants.LOCK_TYPE) {
                id = "/lock" + n;
                Object obj = controller.getServers().get(id);
                if (obj instanceof NettyLockServerHandler) {
                    NettyLockServerHandler lockServer = (NettyLockServerHandler) obj;
                    out = lockServer.handler(ch, buffer);
                }
            } else if (t == net.xinshi.pigeon.netty.common.Constants.CONTROL_TYPE) {
                long cmd = CommonTools.bytes2longJAVA(buffer, Constants.PACKET_PREFIX_LENGTH);
                System.out.println("receive COMMAND : " + cmd);
                int state_word = net.xinshi.pigeon.netty.common.Constants.shift_state_word((int) cmd);
                if (!net.xinshi.pigeon.status.Constants.isAvailable(state_word)) {
                    ByteArrayOutputStream os = new ByteArrayOutputStream();
                    os.write("unknow command".getBytes());
                    out = Tools.buildChannelBuffer(sq, flag, os);
                    return out;
                }
                boolean rc = true;
                if (controller != null) {
                    rc = controller.set_servers_state_word(state_word);
                }
                String command = net.xinshi.pigeon.netty.common.Constants.get_state_string((int) cmd);
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                String info = "change server state word " + command + " return : " + rc;
                os.write(info.getBytes());
                out = Tools.buildChannelBuffer(sq, flag, os);
                logger.info(info);
                if (command.compareToIgnoreCase("stop") == 0) {
                    new Thread(new Runnable() {
                        public void run() {
                            while (true) {
                                try {
                                    Thread.sleep(3000);
                                    logger.warning("server stoped ... ");
                                    System.exit(0);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }).start();
                }
            }
            return out;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void channelConnected(Channel ch) throws Exception {
    }

    public void channelClosed(Channel ch) throws Exception {
        for (Object obj : controller.getServers().values()) {
            if (obj instanceof NettyLockServerHandler) {
                NettyLockServerHandler lockServer = (NettyLockServerHandler) obj;
                lockServer.channelClosed(ch);
            }
        }
    }

    public static void startServers(int listenPort, int nettyPort, String configfile) throws Exception {
        t = new RequestListenerThread(listenPort, configfile);
        controller = t.getController();
        Map<String, Object> servers = controller.getServers();
        for (Object o : servers.values()) {
            try {
                if (o instanceof IServer) {
                    IServer server = (IServer) o;
                    server.start();
                }
            } catch (Exception e) {
                throw e;
            }
        }
        t.setDaemon(false);
        t.start();

        startNettyServers(nettyPort);
    }

    public static void stop() throws Exception {
        t.stopServer();
    }

    static class RequestListenerThread extends Thread {

        private final ServerSocket serversocket;
        private final HttpService httpService;
        private final HttpParams params;
        ServerController controller;
        boolean stop = false;

        public void stopServer() throws Exception {
            stop = true;
            this.serversocket.close();
            interrupt();
            Collection<Object> servers = controller.getServers().values();
            for (Object o : servers) {
                if (o instanceof IServer) {
                    ((IServer) o).stop();
                } else if (o instanceof MinaResourceLockServer) {
                    ((MinaResourceLockServer) o).stop();
                }
            }
        }

        public RequestListenerThread(int port, String configfile) throws Exception {
            params = new BasicHttpParams();
            params.setIntParameter(CoreConnectionPNames.SO_TIMEOUT, 15000);
            params.setIntParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE, 8 * 1024);
            params.setBooleanParameter(CoreConnectionPNames.STALE_CONNECTION_CHECK, false);
            params.setBooleanParameter(CoreConnectionPNames.TCP_NODELAY, true);
            params.setParameter(CoreProtocolPNames.ORIGIN_SERVER, "PIGEONSERVER/3.0");
            BasicHttpProcessor httpproc = new BasicHttpProcessor();
            httpproc.addInterceptor(new ResponseDate());
            httpproc.addInterceptor(new ResponseServer());
            httpproc.addInterceptor(new ResponseContent());
            httpproc.addInterceptor(new ResponseConnControl());
            HttpRequestHandlerRegistry registry = new HttpRequestHandlerRegistry();
            controller = new ServerController(configfile);
            controller.init();
            controller.initDuplicateController(controlFile);
            controller.start(registry);
            registry.register("/status", new StatusHttpRequestHandler());
            registry.register("/download", new DownloadHttpRequestHandler());
            serversocket = new ServerSocket(port);
            serversocket.setReuseAddress(true);
            httpService = new HttpService(httpproc,
                    new DefaultConnectionReuseStrategy(),
                    new DefaultHttpResponseFactory());
            httpService.setParams(this.params);
            httpService.setHandlerResolver(registry);
        }

        public ServerController getController() {
            return controller;
        }

        public void run() {
            Thread.currentThread().setName("RequestListenerThread_run");
            workingQue = new ArrayBlockingQueue<Runnable>(200);
            executor = new ThreadPoolExecutor(30, 200, 300, TimeUnit.SECONDS, workingQue);
            threadCount = new AtomicInteger(0);
            operationsCount = new AtomicLong(0);
            while (!Thread.interrupted()) {
                try {
                    Socket socket = this.serversocket.accept();
                    if (stop) {
                        break;
                    }
                    DefaultHttpServerConnection conn = new DefaultHttpServerConnection();
                    conn.bind(socket, this.params);
                    executor.execute(new WorkerThread(this.httpService, conn));
                } catch (InterruptedIOException ex) {
                    logger.log(Level.INFO, "interrupted:", ex);
                    break;
                } catch (IOException e) {
                    logger.log(Level.INFO, "io exception:", e);
                    break;
                }
            }
            executor.shutdownNow();
            System.out.println("pigeon server is exiting...");
        }
    }

    static class WorkerThread implements Runnable {

        private final HttpService httpservice;
        private final HttpServerConnection conn;

        public WorkerThread(
                final HttpService httpservice,
                final HttpServerConnection conn) {
            super();
            this.httpservice = httpservice;
            this.conn = conn;
        }

        public void run() {
            logger.info("New Connection created by pigeon server.");
            PigeonServer.getThreadCount().getAndIncrement();
            HttpContext context = new BasicHttpContext(null);
            try {
                while (!Thread.interrupted() && conn.isOpen()) {
                    httpservice.handleRequest(conn, context);
                    PigeonServer.getOperationsCount().getAndIncrement();
                }
            } catch (ConnectionClosedException ex) {
                System.err.println("Client closed connection");
            } catch (IOException ex) {
                logger.log(Level.INFO, "I/O Exception:Server Connection closed");
            } catch (HttpException ex) {
                logger.log(Level.SEVERE, "Unrecoverable HTTP protocol violation:", ex);
            } finally {
                PigeonServer.getThreadCount().decrementAndGet();
                try {
                    conn.shutdown();
                } catch (IOException ignore) {
                    ignore.printStackTrace();
                }
            }
        }
    }
}

