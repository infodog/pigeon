package net.xinshi.pigeon.server.standalongserver.atomServer;

import net.xinshi.pigeon.atom.IIntegerAtom;
import net.xinshi.pigeon.atom.impls.dbatom.FastAtom;
import net.xinshi.pigeon.server.standalongserver.BaseServer;
import net.xinshi.pigeon.util.CommonTools;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: zhengxiangyang
 * Date: 11-10-30
 * Time: 上午3:49
 * To change this template use File | Settings | File Templates.
 */
public class AtomServer extends BaseServer {
    IIntegerAtom atom;
    Logger logger = Logger.getLogger("AtomServer");

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

    @Override
    public void stop() {
        try {
            ((FastAtom) atom).stop();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "can not stop atomServer.", e);
        }
    }
}
