package net.xinshi.pigeon.server.standalongserver;

import net.xinshi.pigeon.flexobject.impls.fastsimple.SimpleFlexObjectFactory;
import net.xinshi.pigeon.netty.common.PigeonDecoder;
import net.xinshi.pigeon.netty.server.IServerHandler;
import net.xinshi.pigeon.netty.server.ServerHandler;
import net.xinshi.pigeon.server.standalongserver.atomServer.AtomServer;
import net.xinshi.pigeon.server.standalongserver.atomServer.NettyAtomServerHandler;
import net.xinshi.pigeon.server.standalongserver.flexobjectServer.FlexObjectServer;
import net.xinshi.pigeon.server.standalongserver.flexobjectServer.NettyFlexObjectServerHandler;
import net.xinshi.pigeon.server.standalongserver.idserver.IdServer;
import net.xinshi.pigeon.server.standalongserver.idserver.NettyIdServerHandler;
import net.xinshi.pigeon.server.standalongserver.listServer.ListServer;
import net.xinshi.pigeon.server.standalongserver.listServer.NettyListServerHandler;
import net.xinshi.pigeon.server.standalongserver.lockServer.MinaResourceLockServer;
import net.xinshi.pigeon.util.CommonTools;
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
import org.jboss.netty.buffer.ChannelBuffers;
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
 * User: zxy
 * Date: 2010-7-31
 * Time: 16:42:15
 * To change this template use File | Settings | File Templates.
 */

public class PigeonListener implements IServerHandler {
    static Logger logger = Logger.getLogger(PigeonListener.class.getName());
    static ThreadPoolExecutor executor;
    static ArrayBlockingQueue<Runnable> workingQue;
    static AtomicInteger threadCount;
    static ServerController controller = null;

    public static AtomicLong getOperationsCount() {
        return operationsCount;
    }

    public static void setOperationsCount(AtomicLong operationsCount) {
        PigeonListener.operationsCount = operationsCount;
    }

    static AtomicLong operationsCount;

    public static AtomicInteger getThreadCount() {
        return threadCount;
    }

    public static void setThreadCount(AtomicInteger threadCount) {
        PigeonListener.threadCount = threadCount;
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
            //System.out.println("---------------- pigeon version : " + Constants.version);
            logger.info("pigeon version : " + Constants.version);
            int listenPort = Integer.parseInt(port);
            String[] configServers = new String[]{configFile};
            logger.info("listening:" + listenPort);
            logger.info("listening netty port: " + nettyPort);
            logger.info("using config file:" + configFile);
            startServers(listenPort, Integer.parseInt(nettyPort), configServers);
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
    static SimpleFlexObjectFactory flexObjectFactory;

    public static void startNettyServers(int nettyPort) throws Exception {
        ChannelFactory factory =
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool());
        ServerBootstrap bootstrap = new ServerBootstrap(factory);
        final ExecutionHandler eh = new ExecutionHandler(Executors.newFixedThreadPool(100));
        final ServerHandler sh = new ServerHandler(new PigeonListener());
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

    private ChannelBuffer buildChannelBuffer(int sq, short flag, ByteArrayOutputStream os) {
        ChannelBuffer out = null;
        int len = os.size() + 10;
        out = ChannelBuffers.dynamicBuffer(len);
        out.writeInt(len);
        out.writeInt(sq);
        out.writeShort(flag);
        out.writeBytes(os.toByteArray());
        return out;
    }

    public boolean set_servers_state_word(int state_word) {
        try {
            for (Object obj : controller.getServers().values()) {
                if (obj instanceof FlexObjectServer) {
                    ((FlexObjectServer) obj).set_state_word(state_word);
                } else if (obj instanceof ListServer) {
                    ((ListServer) obj).getFactory().set_state_word(state_word);
                } else if (obj instanceof AtomServer) {
                    ((AtomServer) obj).getAtom().set_state_word(state_word);
                } else if (obj instanceof IdServer) {
                    ((IdServer) obj).getIdgenerator().set_state_word(state_word);
                }
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public ChannelBuffer handler(Channel ch, byte[] buffer) {
        try {
            int sq = CommonTools.bytes2intJAVA(buffer, 4);
            short flag = CommonTools.bytes2shortJAVA(buffer, 8);
            int t = (flag >> 8) & 0xFF;
            int n = flag & 0xFF;
            ChannelBuffer out = null;
            String id = "";

            if (t == net.xinshi.pigeon.netty.common.Constants.FLEXOBJECT_TYPE) {
                id = "/flexobject" + n;
                Object obj = controller.getServers().get(id);
                if (obj instanceof FlexObjectServer) {
                    InputStream is = new ByteArrayInputStream(buffer, 10, buffer.length - 10);
                    ByteArrayOutputStream os = NettyFlexObjectServerHandler.handle((FlexObjectServer) obj, is);
                    if (os == null) {
                        return null;
                    }
                    out = buildChannelBuffer(sq, flag, os);
                }
            } else if (t == net.xinshi.pigeon.netty.common.Constants.LIST_TYPE) {
                id = "/list" + n;
                Object obj = controller.getServers().get(id);
                if (obj instanceof ListServer) {
                    InputStream is = new ByteArrayInputStream(buffer, 10, buffer.length - 10);
                    ByteArrayOutputStream os = NettyListServerHandler.handle((ListServer) obj, is);
                    if (os == null) {
                        return null;
                    }
                    out = buildChannelBuffer(sq, flag, os);
                }
            } else if (t == net.xinshi.pigeon.netty.common.Constants.ATOM_TYPE) {
                id = "/atom" + n;
                Object obj = controller.getServers().get(id);
                if (obj instanceof AtomServer) {
                    InputStream is = new ByteArrayInputStream(buffer, 10, buffer.length - 10);
                    ByteArrayOutputStream os = NettyAtomServerHandler.handle((AtomServer) obj, is);
                    if (os == null) {
                        return null;
                    }
                    out = buildChannelBuffer(sq, flag, os);
                }
            } else if (t == net.xinshi.pigeon.netty.common.Constants.ID_TYPE) {
                id = "/idserver" + n;
                Object obj = controller.getServers().get(id);
                if (obj instanceof IdServer) {
                    InputStream is = new ByteArrayInputStream(buffer, 10, buffer.length - 10);
                    ByteArrayOutputStream os = NettyIdServerHandler.handle((IdServer) obj, is);
                    if (os == null) {
                        return null;
                    }
                    out = buildChannelBuffer(sq, flag, os);
                }
            } else if (t == net.xinshi.pigeon.netty.common.Constants.CONTROL_TYPE) {
                long cmd = CommonTools.bytes2longJAVA(buffer, 10);
                System.out.println("receive COMMAND : " + cmd);
                int state_word = net.xinshi.pigeon.netty.common.Constants.shift_state_word((int) cmd);
                if (!net.xinshi.pigeon.status.Constants.isAvailable(state_word)) {
                    ByteArrayOutputStream os = new ByteArrayOutputStream();
                    os.write("unknow command".getBytes());
                    out = buildChannelBuffer(sq, flag, os);
                    return out;
                }
                boolean rc = set_servers_state_word(state_word);
                String command = net.xinshi.pigeon.netty.common.Constants.get_state_string((int) cmd);
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                String info = "change server state word " + command + " return : " + rc;
                os.write(info.getBytes());
                out = buildChannelBuffer(sq, flag, os);
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
    }

    public static void startServers(int listenPort, int nettyPort, String[] configServers) throws Exception {
        t = new RequestListenerThread(listenPort, configServers);
        controller = t.getController();
        Map<String, Object> servers = controller.getServers();
        for (Object o : servers.values()) {
            try {
                if (o instanceof IServer) {
                    IServer server = (IServer) o;
                    server.start();
                    if (server instanceof FlexObjectServer) {
                        flexObjectFactory = (SimpleFlexObjectFactory) ((FlexObjectServer) server).getFlexObjectFactory();
                    }
                }
            } catch (Exception e) {
                throw e;
            }
        }

        startNettyServers(nettyPort);

        //not daemon thread , means when this thread is alive the jvm will not exit;
        t.setDaemon(false);
        t.start();

    }

    public static SimpleFlexObjectFactory getFlexObjectFactory() {
        return flexObjectFactory;
    }

    public static void stop() throws IOException {
        t.stopServer();
    }

    static class RequestListenerThread extends Thread {
        private final ServerSocket serversocket;
        private final HttpParams params;
        private final HttpService httpService;
        boolean stop;
        ServerController controller;

        public void stopServer() throws IOException {
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

        public RequestListenerThread(int port, final String[] configServers) throws Exception {
            this.params = new BasicHttpParams();
            this.params
                    .setIntParameter(CoreConnectionPNames.SO_TIMEOUT, 15000)
                    .setIntParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE, 8 * 1024)
                    .setBooleanParameter(CoreConnectionPNames.STALE_CONNECTION_CHECK, false)
                    .setBooleanParameter(CoreConnectionPNames.TCP_NODELAY, true)
                    .setParameter(CoreProtocolPNames.ORIGIN_SERVER, "PIGEONSERVER/3.0");

            // Set up the HTTP protocol processor
            BasicHttpProcessor httpproc = new BasicHttpProcessor();
            httpproc.addInterceptor(new ResponseDate());
            httpproc.addInterceptor(new ResponseServer());
            httpproc.addInterceptor(new ResponseContent());
            httpproc.addInterceptor(new ResponseConnControl());

            // Set up request handlers
            HttpRequestHandlerRegistry registry = new HttpRequestHandlerRegistry();
            controller = new ServerController();
            controller.setConfigServer(configServers);
            controller.start(registry);
            this.serversocket = new ServerSocket(port);
            this.serversocket.setReuseAddress(true);
/*               String host = controller.getHost();
               SocketAddress socketAddress = new InetSocketAddress(host,port);
               this.serversocket = new ServerSocket(port,socketAddress);
               //this.serversocket.bind(socketAddress);
*/

            // Set up the HTTP service
            this.httpService = new HttpService(
                    httpproc,
                    new DefaultConnectionReuseStrategy(),
                    new DefaultHttpResponseFactory());
            this.httpService.setParams(this.params);
            this.httpService.setHandlerResolver(registry);
            stop = false;
        }

        public ServerController getController() {
            return controller;
        }

        public void run() {
            workingQue = new ArrayBlockingQueue<Runnable>(200);
            executor = new ThreadPoolExecutor(30, 200, 300, TimeUnit.SECONDS, workingQue);
            threadCount = new AtomicInteger(0);
            operationsCount = new AtomicLong(0);
            while (!Thread.interrupted()) {
                try {
                    // Set up HTTP connection
                    Socket socket = this.serversocket.accept();
                    if (stop) {
                        break;
                    }
                    DefaultHttpServerConnection conn = new DefaultHttpServerConnection();
                    conn.bind(socket, this.params);
                    // Start worker thread
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
            //System.out.println("New connection thread");
            logger.info("New Connection created by pigeon server.");

            PigeonListener.getThreadCount().getAndIncrement();
            HttpContext context = new BasicHttpContext(null);
            try {
                while (!Thread.interrupted() && this.conn.isOpen()) {
                    this.httpservice.handleRequest(this.conn, context);
                    PigeonListener.getOperationsCount().getAndIncrement();
                }
            } catch (ConnectionClosedException ex) {
                System.err.println("Client closed connection");
            } catch (IOException ex) {
                //System.err.println("I/O error: " + ex.getMessage());

                //其实这个是正常的，没有问题。
                logger.log(Level.INFO, "I/O Exception:Server Connection closed");
            } catch (HttpException ex) {
                //System.err.println("Unrecoverable HTTP protocol violation: " + ex.getMessage());
                logger.log(Level.SEVERE, "Unrecoverable HTTP protocol violation:", ex);
            } finally {
                PigeonListener.getThreadCount().decrementAndGet();
                try {
                    this.conn.shutdown();
                } catch (IOException ignore) {
                    ignore.printStackTrace();
                }
            }
        }

    }
}

