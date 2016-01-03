package net.xinshi.pigeon.server.distributedserver.lockserver;

import org.apache.commons.lang.StringUtils;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午5:44
 * To change this template use File | Settings | File Templates.
 */

public class MinaLockServerHandler extends IoHandlerAdapter {

    private final static Logger LOGGER = LoggerFactory.getLogger(MinaLockServerHandler.class);
    IAsynLockServer lockServer;
    ConcurrentHashMap sessions = new ConcurrentHashMap();

    public MinaLockServerHandler() {
        lockServer = new AsynLockServer();
    }

    @Override
    public void sessionCreated(IoSession session) {
        session.getConfig().setIdleTime(IdleStatus.BOTH_IDLE, 100000000);
    }

    @Override
    public void sessionClosed(IoSession session) throws Exception {
        synchronized (session) {
            Set locks = (Set) session.getAttribute("locks");
            for (Iterator it = locks.iterator(); it.hasNext();) {
                String lock = (String) it.next();
                String[] parts = lock.split("\\$");
                String resId = parts[0];
                String threadId = parts[1];
                try {
                    this.unLock(resId, threadId, session);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                lockServer.removeWaitingThread(resId, threadId);
                sessions.remove(lock);
            }
        }

    }

    @Override
    public void sessionOpened(IoSession session) throws Exception {
        LOGGER.info("OPENED");
    }

    @Override
    public void sessionIdle(IoSession session, IdleStatus status) {
        LOGGER.info("*** IDLE #" + session.getIdleCount(IdleStatus.BOTH_IDLE) + " ***");
    }

    @Override
    public void exceptionCaught(IoSession session, Throwable cause) {
        session.close(true);
    }

    @Override
    synchronized public void messageReceived(IoSession session, Object message) throws Exception {
        LOGGER.info("Received : " + message);
        if (StringUtils.isBlank((String) message)) {
            return;
        }
        String[] parts = parseMessage((String) message);
        String action = parts[0];
        String resId = parts[1];
        String threadId = parts[2];
        if (StringUtils.equals(action, "lock")) {
            synchronized (sessions) {
                sessions.put(resId + "$" + threadId, session);
            }
            synchronized (session) {
                Set locks = (Set) session.getAttribute("locks");
                if (locks == null) {
                    locks = new ConcurrentHashSet();
                    session.setAttribute("locks", locks);
                }
                locks.add(resId + "$" + threadId);
            }
            if (lockServer.lock(resId, threadId)) {
                session.write("locked$" + resId + "$" + threadId);
            }
        } else if (StringUtils.equals(action, "unlock")) {
            this.unLock(resId, threadId, session);
        } else if (StringUtils.equals(action, "reportLocks")) {
            synchronized (sessions) {
                session.write("reportLocks$" + threadId + "$" + this.lockServer.getLocks());
            }
        }
    }

    private void unLock(String resId, String threadId, IoSession session) throws Exception {
        String[] nextThreadId = new String[1];
        boolean bSuccess = lockServer.unlock(resId, threadId, nextThreadId);
        if (bSuccess) {
            String tid = nextThreadId[0];
            if (tid != null) {
                IoSession s = (IoSession) sessions.get(resId + "$" + tid);
                if (s == null) {
                    return;
                }
                s.write("locked$" + resId + "$" + tid);
            }
            synchronized (sessions) {
                sessions.remove(resId + "$" + threadId);
            }
            synchronized (session) {
                Set locks = (Set) session.getAttribute("locks");
                if (locks == null) {
                    locks = new ConcurrentHashSet();
                    session.setAttribute("locks", locks);
                }
                locks.remove(resId + "$" + threadId);
            }
        }
    }

    String[] parseMessage(String message) {
        return message.split("\\$");
    }
}


