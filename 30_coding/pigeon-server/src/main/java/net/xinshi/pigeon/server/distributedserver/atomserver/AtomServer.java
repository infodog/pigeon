package net.xinshi.pigeon.server.distributedserver.atomserver;

import net.xinshi.pigeon.atom.IIntegerAtom;
import net.xinshi.pigeon.atom.impls.dbatom.FastAtom;
import net.xinshi.pigeon.server.distributedserver.BaseServer;
import net.xinshi.pigeon.server.distributedserver.util.Tools;
import net.xinshi.pigeon.util.CommonTools;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午3:55
 * To change this template use File | Settings | File Templates.
 */

public class AtomServer extends BaseServer {

    IIntegerAtom atom;
    Logger logger = Logger.getLogger(AtomServer.class.getName());

    public ByteArrayOutputStream doSyncDataItems(InputStream in) throws Exception {
        while (true) {
            long version = 0L;
            String line;
            try {
                version = CommonTools.readLong(in);
                byte[] data = CommonTools.readBytes(in);
                line = new String(data, "UTF-8");
            } catch (Exception e) {
                break;
            }
            try {
                writes++;
                ((FastAtom) getAtom()).writeVersionLogAndCache(version, line);
            } catch (Exception e) {
                throw e;
            }
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "ok");
        return out;
    }

    public ByteArrayOutputStream doPullDataItems(InputStream in) throws Exception {
        while (true) {
            long min = 0L;
            long max = 0L;
            try {
                min = CommonTools.readLong(in);
                max = CommonTools.readLong(in);
                if (min < 1 || max < min) {
                    throw new Exception("doPullDataItems min or max error ... ");
                }
            } catch (Exception e) {
                break;
            }
            try {
                return ((FastAtom) getAtom()).pullDataItems(min, max);
            } catch (Exception e) {
                throw e;
            }
        }
        throw new Exception("doPullDataItems error ...... ");
    }

    public ByteArrayOutputStream doCommand(InputStream in) throws Exception {
        try {
            String cmd = CommonTools.readString(in);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            if (cmd.equals("version")) {
                long ver = ((FastAtom) getAtom()).verLogger.getVersion();
                CommonTools.writeString(out, String.valueOf(ver));
                return out;
            }
            return null;
        } catch (Exception e) {
            throw e;
        }
    }

    public void doGetValue(InputStream is, ByteArrayOutputStream os) throws Exception {
        String name = CommonTools.readString(is);
        try {
            Long r = atom.get(name);
            String result = "";
            if (r == null) {
                result = "";
            } else {
                result = String.valueOf(r);
            }
            CommonTools.writeString(os, "ok");
            CommonTools.writeString(os, result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void doCreateAndSet(InputStream is, ByteArrayOutputStream os) throws Exception {
        String name = CommonTools.readString(is);
        String initValue = CommonTools.readString(is);
        int value = 0;
        try {
            Tools.checkNameLength(name);
            value = Integer.parseInt(initValue);
            atom.createAndSet(name, value);
            CommonTools.writeString(os, "ok");
        } catch (Exception e) {
            e.printStackTrace();
            CommonTools.writeString(os, e.getMessage());
        }
    }

    public void doGreaterAndInc(InputStream is, ByteArrayOutputStream os) throws Exception {
        String name = CommonTools.readString(is);
        String testValue = CommonTools.readString(is);
        String incValue = CommonTools.readString(is);
        int value = 0;
        int inc = 0;
        try {
            value = Integer.parseInt(testValue);
            inc = Integer.parseInt(incValue);
            long r = atom.greaterAndIncReturnLong(name, value, inc);
            CommonTools.writeString(os, "ok");
            CommonTools.writeString(os, "" + r);
        } catch (Exception e) {
            if (e.getMessage().compareTo("return false") == 0) {
                CommonTools.writeString(os, "failed");
                return;
            }
            e.printStackTrace();
            CommonTools.writeString(os, e.getMessage());
        }
    }

    public void doLessAndInc(InputStream is, ByteArrayOutputStream os) throws Exception {
        String name = CommonTools.readString(is);
        String testValue = CommonTools.readString(is);
        String incValue = CommonTools.readString(is);
        int value = 0;
        int inc = 0;
        try {
            value = Integer.parseInt(testValue);
            inc = Integer.parseInt(incValue);
            long r = atom.lessAndIncReturnLong(name, value, inc);
            CommonTools.writeString(os, "ok");
            CommonTools.writeString(os, "" + r);
        } catch (Exception e) {
            if (e.getMessage().compareTo("return false") == 0) {
                CommonTools.writeString(os, "failed");
                return;
            }
            e.printStackTrace();
            CommonTools.writeString(os, e.getMessage());
        }
    }

    public void doGetAtoms(InputStream is, ByteArrayOutputStream os) throws Exception {
        List<String> atomIds = new Vector<String>();
        String atomId = CommonTools.readString(is);
        while (atomId != null) {
            atomIds.add(atomId);
            atomId = CommonTools.readString(is);
        }
        List<Long> atoms = atom.getAtoms(atomIds);
        CommonTools.writeString(os, "ok");
        for (Long v : atoms) {
            if (v != null) {
                CommonTools.writeString(os, "" + v);
            } else {
                CommonTools.writeString(os, "null");
            }
        }
    }

    public IIntegerAtom getAtom() {
        return atom;
    }

    public void setAtom(IIntegerAtom atom) {
        this.atom = atom;
    }

    public Map getStatusMap() {
        return ((FastAtom) atom).getStatusMap();
    }

    @Override
    public void stop() {
        try {
            ((FastAtom) atom).stop();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "can not stop atomServer.", e);
        }
    }

}

