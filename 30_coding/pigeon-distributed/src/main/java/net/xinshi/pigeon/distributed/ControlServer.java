package net.xinshi.pigeon.distributed;

import net.xinshi.pigeon.distributed.handler.handleDataServer;
import net.xinshi.pigeon.distributed.manager.PigeonNodesManager;
import net.xinshi.pigeon.netty.common.PigeonDecoder;
import net.xinshi.pigeon.netty.server.IServerHandler;
import net.xinshi.pigeon.netty.server.ServerHandler;
import net.xinshi.pigeon.util.CommonTools;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import static org.jboss.netty.channel.Channels.pipeline;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-3
 * Time: 上午11:06
 * To change this template use File | Settings | File Templates.
 */

public class ControlServer implements IServerHandler {

    static Logger logger = Logger.getLogger(ControlServer.class.getName());
    static PigeonNodesManager nodesMgr = new PigeonNodesManager();
    static handleDataServer handleServer = new handleDataServer(nodesMgr);

    public static void main(String[] args) throws Exception {
        try {

            System.out.println(System.getProperty("user.dir"));
            nodesMgr.init("pigeon-distributed\\src\\main\\resources\\pigeonnodes.conf");

            int port = Constants.CONTROL_SERVER_PORT;
            String config = Constants.CONFIG_FILE;
            if (args.length > 0) {
                port = Integer.parseInt(args[0]);
            }
            if (args.length > 1) {
                config = args[1];
            }

            startNettyServers(port);

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

    public static void startNettyServers(int nettyPort) throws Exception {
        ChannelFactory factory =
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool());
        ServerBootstrap bootstrap = new ServerBootstrap(factory);
        final ExecutionHandler eh = new ExecutionHandler(Executors.newFixedThreadPool(100));
        final ServerHandler sh = new ServerHandler(new ControlServer());
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
        ChannelBuffer out;
        int len = os.size() + Constants.PACKET_PREFIX_LENGTH;
        out = ChannelBuffers.dynamicBuffer(len);
        out.writeInt(len);
        out.writeInt(sq);
        out.writeShort(flag);
        out.writeBytes(os.toByteArray());
        return out;
    }

    public boolean set_servers_state_word(int state_word) {
        try {
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    private ChannelBuffer quit(int sq, short flag, byte[] buffer) throws Exception {
        ChannelBuffer out = null;
        long cmd = CommonTools.bytes2longJAVA(buffer, Constants.PACKET_PREFIX_LENGTH);
        System.out.println("receive COMMAND : " + cmd);
        int state_word = net.xinshi.pigeon.netty.common.Constants.shift_state_word((int) cmd);
        if (!Constants.isAvailable(state_word)) {
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
        return out;
    }

    public ChannelBuffer handler(Channel ch, byte[] buffer) {
        try {
            int sq = CommonTools.bytes2intJAVA(buffer, 4);
            short flag = CommonTools.bytes2shortJAVA(buffer, 8);
            int t = (flag >> 8) & 0xFF;
            int n = flag & 0xFF;

            if (t == net.xinshi.pigeon.netty.common.Constants.CONTROL_TYPE) {
                if (n == Constants.ACTION_LOGIN) {
                    ByteArrayOutputStream os = handleServer.handler(buffer);
                    return buildChannelBuffer(sq, flag, os);
                }
                if (n == Constants.ACTION_HEARTBEAT) {
                    ByteArrayOutputStream os = handleServer.handler(buffer);
                    return buildChannelBuffer(sq, flag, os);
                }
                if (n == Constants.ACTION_NODES_INFO) {
                    ByteArrayOutputStream os = handleServer.handler(buffer);
                    return buildChannelBuffer(sq, flag, os);
                }
                return quit(sq, flag, buffer);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void channelConnected(Channel ch) throws Exception {
    }

    public void channelClosed(Channel ch) throws Exception {
    }

}

