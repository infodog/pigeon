package net.xinshi.pigeon.netty.client;

import net.xinshi.pigeon.netty.common.PigeonDecoder;
import net.xinshi.pigeon.netty.common.PigeonFuture;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.jboss.netty.channel.Channels.pipeline;

public class Client extends Thread implements Comparable<Client> {

    static  Logger logger = Logger.getLogger(Client.class.getName());

    private String host;
    private int port;
    private int conns = 10;
    private volatile int sequence = 0;
    private volatile int index = 0;
    private ClientBootstrap bootstrap = null;
    private Vector<Channel> vecChannels = new Vector<Channel>();
    private LinkedHashMap<Integer, PigeonFuture> mapFutures = new LinkedHashMap<Integer, PigeonFuture>();
    private boolean startup = false;

    public boolean isStartup() {
        return startup;
    }

    public void setStartup(boolean startup) {
        this.startup = startup;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getConns() {
        return conns;
    }

    public void setConns(int conns) {
        this.conns = conns;
    }

    public Client(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public Client(String host, int port, int conns) {
        this.host = host;
        this.port = port;
        this.conns = conns;
    }

    private boolean init_channels() {
        synchronized (vecChannels) {
            for (Channel ch : vecChannels) {
                ch.close();
            }
            vecChannels.clear();
            for (int i = 0; i < conns; i++) {
                try {
                    Channel channel = null;
                    ChannelFuture connectFuture = bootstrap.connect(new InetSocketAddress(host, port));
                    while(true){
                        connectFuture.awaitUninterruptibly();
                        if(connectFuture.isDone()){
                            channel = connectFuture.getChannel();
                            if(channel.isOpen()){
                                vecChannels.add(channel);
                                logger.log(Level.INFO,"channel connected,channel id is " + channel.getId());

                            }
                            break;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (vecChannels.size() == conns) {
                return true;
            }
            if(vecChannels.size()>0){
                logger.warning("try to make " + conns + "connections, succeeded connections were " + vecChannels.size());
                return true;
            }
            return false;
        }
    }

    public boolean init() {
        synchronized (vecChannels) {
            try {
                bootstrap = new ClientBootstrap(
                        new NioClientSocketChannelFactory(
                                Executors.newCachedThreadPool(),
                                Executors.newCachedThreadPool()));
                final ExecutionHandler eh = new ExecutionHandler(Executors.newFixedThreadPool(conns));
                final Client me = this;
                bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
                    public ChannelPipeline getPipeline() throws Exception {
                        ChannelPipeline cpl = pipeline(new PigeonDecoder(), eh, new ClientHandler(me));
                        return cpl;
                    }
                });
//                if (!init_channels()) {
//                    System.out.println("netty init channels - server not online ... ");
//                }
            } catch (Exception e) {
                e.printStackTrace();
                vecChannels.clear();
                return false;
            }
            this.start();
            // System.out.println("netty client init ... ");
            startup = true;
            return true;
        }
    }

    public synchronized int getSequence() {
        ++sequence;
        if (sequence == Integer.MAX_VALUE) {
            sequence = 1;
        }
        return sequence;
    }

    public int getIndex() {
        synchronized (vecChannels) {
            if (vecChannels.size() < 1) {
                return -1;
            }
            ++index;
            if (index >= vecChannels.size()) {
                index = 0;
            }
            return index;
        }
    }

    public void addPigeonFuture(Integer sq, PigeonFuture pf) {
        synchronized (mapFutures) {
            mapFutures.put(sq, pf);
        }
    }

    public void delPigeonFuture(Integer sq) {
        synchronized (mapFutures) {
            mapFutures.remove(sq);
        }
    }

    public void channelClosed(Channel ch) throws Exception {
        synchronized (vecChannels) {
            int index = vecChannels.indexOf(ch);
            if (index < 0) {
                return;
            }
            synchronized (mapFutures) {
                for (PigeonFuture pf : mapFutures.values()) {
                    if (pf.getIndex() == index) {
                        synchronized (pf) {
                            pf.setCancel(true);
                            pf.notify();
                        }
                    }
                }
            }
        }
        ch.disconnect();
    }

    public PigeonFuture notifyPigeonFuture(Integer sq, short flag, byte[] data) {
        PigeonFuture pf;
        synchronized (mapFutures) {
            pf = mapFutures.get(sq);
        }
        if (pf != null) {
            pf.setFlag(flag);
            pf.setData(data);
            synchronized (pf) {
                pf.setComplete(true);
                pf.notify();
            }
        }
        return pf;
    }

    private Channel resetChannel(int index) {
        Channel ch = null;
        Channel channel = null;
        synchronized (vecChannels) {
            ch = vecChannels.get(index);
            ChannelFuture connectFuture = bootstrap.connect(new InetSocketAddress(host, port));
            channel = connectFuture.awaitUninterruptibly().getChannel();
            vecChannels.set(index, channel);
        }
        ch.close();
        return channel;
    }

    public PigeonFuture send(byte[] data) {
        int index = this.getIndex();
        if (index == -1) {
            init_channels();
            return null;
        }
        Channel ch = null;
        synchronized (vecChannels) {
            ch = vecChannels.get(index);
        }
        if (!ch.isConnected()) {
            resetChannel(index);
            return null;
        }
        PigeonFuture pf = new PigeonFuture(this);
        pf.setIndex(index);
        int sq = this.getSequence();
        pf.setSequence(sq);
        pf.setData(data);
        addPigeonFuture(sq, pf);
        int len = data.length + 8;
        ChannelBuffer dcb = ChannelBuffers.dynamicBuffer(len);
        dcb.writeInt(len);
        dcb.writeInt(sq);
        dcb.writeBytes(data);
        synchronized (ch) {
            try {
                ChannelFuture cf = ch.write(dcb);
                // cf.awaitUninterruptibly();
            } catch (Exception e) {
                e.printStackTrace();
                delPigeonFuture(sq);
                resetChannel(index);
                return null;
            }
        }
        return pf;
    }

    public PigeonFuture send(short flag, byte[] data) {
        int index = this.getIndex(); //10��Connection�е�һ��
        if (index == -1) {
            boolean success = init_channels();
            System.out.println("netty client send,no active connections, init_channels,success=" + success);
            return null;
        }
        Channel ch = null;
        synchronized (vecChannels) {
            ch = vecChannels.get(index);
        }
        if (!ch.isConnected()) {
            ch = resetChannel(index);
            if (!ch.isConnected()) {
                System.out.println("netty client send not connected reset_channel");
                return null;
            }
        }
        PigeonFuture pf = new PigeonFuture(this);
        pf.setIndex(index);
        int sq = this.getSequence();
        pf.setSequence(sq);
        pf.setData(data);
        addPigeonFuture(sq, pf);
        int len = data.length + 10;
        ChannelBuffer dcb = ChannelBuffers.dynamicBuffer(len);
        dcb.writeInt(len);
        dcb.writeInt(sq);
        dcb.writeShort(flag);
        dcb.writeBytes(data);
        synchronized (ch) {
            try {
                ChannelFuture cf = ch.write(dcb);
                // cf.awaitUninterruptibly();
            } catch (Exception e) {
                e.printStackTrace();
                delPigeonFuture(sq);
                resetChannel(index);
                return null;
            }
        }
        return pf;
    }

    public void run() {
        final long TIME_OUT_SECENDS = 1000 * 3600;
        Thread.currentThread().setName("Netty_Client_run");
        while (true) {
            try {
                long time = System.currentTimeMillis();
                LinkedList<Integer> listSQ = new LinkedList<Integer>();
                int ix = 0;
                int iy = 0;
                synchronized (mapFutures) {
                    ix = mapFutures.size();
                    for (Iterator it = mapFutures.values().iterator(); it.hasNext(); ) {
                        PigeonFuture pf = (PigeonFuture) it.next();
                        if (pf.getBornTime() + TIME_OUT_SECENDS < time) {
                            listSQ.add(pf.getSequence());
                        } else {
                            break;
                        }
                    }
                    for (Iterator<Integer> it = listSQ.listIterator(); it.hasNext(); ) {
                        Integer sq = it.next();
                        mapFutures.remove(sq);
                    }
                    iy = mapFutures.size();
                }
                if (ix > 100) {
                    System.out.println(Calendar.getInstance().getTime().toString() + " ix sequence : " + ix + " , iy sequence : " + iy);
                }
                Thread.sleep(TIME_OUT_SECENDS);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public InputStream Commit(short flag, ByteArrayOutputStream out) {
        InputStream in;
        try {
            PigeonFuture pf = send(flag, out.toByteArray());
            if (pf == null) {
                pf = send(flag, out.toByteArray());
            }
            boolean ok = false;
            try {
                if (pf != null) {
                    ok = pf.waitme(1000 * 60);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (pf == null) {
                throw new Exception("netty commit pf == null");
            }
            if (!ok) {
                throw new Exception("netty commit server timeout");
            }
            in = new ByteArrayInputStream(pf.getData(), 10, pf.getData().length - 10);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return in;
    }

    public int compareTo(Client o) {
        int rc = this.getHost().compareToIgnoreCase(o.getHost());
        if (rc == 0) {
            return this.getPort() - o.getPort();
        }
        return rc;
    }

}
