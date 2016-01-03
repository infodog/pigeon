package net.xinshi.pigeon.client.minaclient;

import net.xinshi.pigeon.resourcelock.IResourceLock;
import org.apache.commons.lang.StringUtils;
import org.apache.mina.core.RuntimeIoException;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.Hashtable;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: zhengxiangyang
 * Date: 11-11-1
 * Time: 下午12:01
 * To change this template use File | Settings | File Templates.
 */
public class AsynLockClient implements IResourceLock {

    private IoSession session;
    private String ip;
    private String port;
    private ConnectFuture cf;
    private final Hashtable<String, Lock> locks;
    private final int maxRetry = 10;
    private final int interval = 500;
    private Logger logger = LoggerFactory.getLogger(AsynLockClient.class);
    private Boolean connected = false;
    private long timeout = 120 * 1000L;
    private Hashtable<String, LockResult> serverLockResult;
    public boolean bExit = false;

    Thread connectThread = null;

    public boolean isbExit() {
        return bExit;
    }

    public void setbExit(boolean bExit) {
        this.bExit = bExit;
    }

    synchronized public void stop() throws InterruptedException {
        if (bExit) return;
        setbExit(true);
        if (connectThread != null) {
            connectThread.join(2000);
            connectThread = null;
        }
    }

    public class Lock {
        String status;
        int count;

        public Lock() {
            count = 0;
            status = null;
        }

        public String getStatus() {
            return status;
        }
    }

    class LockResult {
        String lockstring;
    }

    void connect() {

        // Create TCP/IP connector.
        NioSocketConnector connector = new NioSocketConnector(1);

        // Set connect timeout.
        connector.setConnectTimeoutMillis(30 * 1000L);
        connector.getFilterChain().addLast("codec", new ProtocolCodecFilter(new TextLineCodecFactory(Charset.forName("UTF-8"), "\n", "\n")));
        // Start communication.
        connector.setHandler(new AsynLockClientHandler());
        while (!bExit) {
            try {
                ConnectFuture cf = connector.connect(new InetSocketAddress(ip, Integer.parseInt(port)));

                // Wait for the connection attempt to be finished.
                cf.awaitUninterruptibly();
                session = cf.getSession();
                setConnected(true);
                cf.getSession().getCloseFuture().awaitUninterruptibly();
                setConnected(false);

            } catch (RuntimeIoException e) {
                e.printStackTrace();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    if (!bExit) {
                        e1.printStackTrace();
                    }
                }
            } finally {
                clear();
            }
        }
    }

    public AsynLockClient(String ip, String port) {
        this.ip = ip;
        this.port = port;
        locks = new Hashtable();
        serverLockResult = new Hashtable<String, LockResult>();
        connectThread = new Thread(new Runnable() {
            public void run() {
                System.out.println("mina lock run() connect ...... ");
                connect();
            }
        });
        connectThread.start();
    }

    synchronized Boolean isConnected() {
        return connected;
    }

    synchronized void setConnected(boolean connected) {
        this.connected = connected;
    }

    boolean ensureConnected() {
        if (bExit) {
            return false;
        }
        for (int i = 0; i < 100; i++) {
            if (bExit) return false;
            if (isConnected() == false) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            } else {
                return true;
            }
        }
        return false;
    }

    public void Lock(String resId) throws Exception {
        if (!ensureConnected()) {
            throw new Exception("can't connect lock server");
        }
        String threadId = IdentityKeygen.get();
        String lockid = resId + "$" + threadId;

        Lock lock;
        synchronized (locks) {
            lock = (Lock) locks.get(lockid);
            if (lock != null) {
                lock.count++;
                return;
            } else {
                lock = new Lock();
                lock.count++;
                locks.put(lockid, lock);
            }
        }
        synchronized (lock) {
            if (lock.count == 1) {
                //只用第一次锁时，才真正的去服务器锁，第二次的时候可以认为已经锁住了。

                synchronized (session) {
                    session.write("lock$" + lockid);
                }
            }
            while (lock.status == null) {
                lock.wait(timeout);
                if (lock.status == null) {
                    lock.count--;
                    if (lock.count <= 0) {
                        locks.remove(lockid);
                    }
                    lock.status = "lock time out";
                }
            }
            if (lock.status.equals("ok")) {
                return;
            } else {
                throw new Exception(lock.status);
            }
        }
    }


    public void Unlock(String resId) throws Exception {
        if (!isConnected()) {
            throw new Exception("lock connection reset");
        }
        String threadId = IdentityKeygen.get();
        String lockid = resId + "$" + threadId;
        synchronized (locks) {
            Lock lock = (Lock) locks.get(lockid);
            if (lock != null) {
                lock.count--;
                if (lock.count > 0) {
                    return;
                } else {
                    synchronized (session) {
                        session.write("unlock$" + lockid);
                    }
                    locks.remove(lockid);
                }
            }
        }
    }

    public void releaseServerLock(String lockid) {
        synchronized (session) {
            session.write("unlock$" + lockid);
        }
    }

    public void releaseLocalLock(String lockid) throws Exception {
        synchronized (locks) {
            Lock lock = locks.get(lockid);
            if (lock != null) {
                synchronized (lock) {
                    if (lock != null && lock.status == null) {
                        locks.remove(lockid);
                        lock.status = "kill lock";
                        lock.notifyAll();
                    }
                }
            }
        }
    }

    public String getServerLocks() throws Exception {
        if (!ensureConnected()) {
            throw new Exception("can't connect lock server");
        }
        String threadId = IdentityKeygen.get();
        //String threadName = Thread.currentThread().getName();
        LockResult result;

        synchronized (serverLockResult) {
            result = new LockResult();
            serverLockResult.put(threadId, result);
        }

        synchronized (session) {
            session.write("reportLocks$0$" + threadId);
        }

        synchronized (result) {
            result.wait(10 * 1000);
            serverLockResult.remove(threadId);
            return result.lockstring;
        }
    }

    public Hashtable<String, Lock> getLocks() {
        synchronized (locks) {
            return new Hashtable<String, Lock>(locks);
        }
    }


    class AsynLockClientHandler extends IoHandlerAdapter {
        @Override
        public void sessionOpened(IoSession session) {
            session.getConfig().setIdleTime(IdleStatus.READER_IDLE, 100);
        }

        @Override
        public void exceptionCaught(IoSession session, Throwable cause) throws java.lang.Exception {
            super.exceptionCaught(session, cause);
        }

        @Override
        public void sessionClosed(IoSession session) throws Exception {
            //TODO:add
        }

        @Override
        public void sessionIdle(IoSession session, IdleStatus status) {
            if (status == IdleStatus.READER_IDLE) {
            }
        }

        @Override
        synchronized public void messageReceived(IoSession session, Object message) {
            String msg = (String) message;
//            System.out.println("##################   AsynLockClient  ##################");
//            System.out.println("AsynLockClient.messageReceived message=" + message);

            //System.out.println("msg:" + msg);
            logger.debug("msg:" + msg);
            if (StringUtils.isBlank(msg)) {
                return;
            }
            String[] parts = msg.split("\\$");
//            if (parts.length != 3) {
//                logger.error("wrong format:" + msg);
//                return;
//            }
            String action = parts[0];

            if (StringUtils.equals(action, "locked")) {
                String resId = parts[1];
                String threadId = parts[2];
                synchronized (locks) {
                    Lock lock = (Lock) locks.get(resId + "$" + threadId);
                    synchronized (lock) {
                        lock.status = "ok";
                        lock.notify();
                    }
                }
            } else if (StringUtils.equals(action, "reportLocks")) {
                try {
                    String threadId = parts[1];
                    String lockstring = parts[2];
                    LockResult result = serverLockResult.get(threadId);
                    if (result != null) {
                        result.lockstring = lockstring;
                        synchronized (result) {
                            result.notifyAll();
                        }
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    private void clear() {
        session = null;
        synchronized (locks) {
            for (Map.Entry<String, Lock> lockEntry : locks.entrySet()) {
                Lock lock = lockEntry.getValue();
                synchronized (lock) {
                    lock.status = "sesseionClose";
                    lock.notify();
                }
            }
            locks.clear();
        }
    }
}

