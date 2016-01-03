package net.xinshi.pigeon.client.nettyclient.atomclient;

import net.xinshi.pigeon.atom.IIntegerAtom;
import net.xinshi.pigeon.netty.client.Client;
import net.xinshi.pigeon.util.CommonTools;
import org.apache.commons.lang.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Vector;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-1-31
 * Time: 下午2:43
 * To change this template use File | Settings | File Templates.
 */

public class NettyAtom implements IIntegerAtom {
    String host = "127.0.0.1";
    int port = 8878;
    Client ch = null;

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

    public Client getCh() {
        return ch;
    }

    public void setCh(Client ch) {
        this.ch = ch;
    }

    public boolean createAndSet(String name, Integer initValue)
            throws Exception {

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "createAndSet");
        CommonTools.writeString(out, name);
        CommonTools.writeString(out, "" + initValue);

        try {

            InputStream in = ch.Commit(net.xinshi.pigeon.netty.common.Constants.ATOM_CODE, out);
            if (in == null) {
                throw new Exception("server error; timeout");
            }
            String s = CommonTools.readString(in);
            s = StringUtils.trim(s);
            if (s.equals("ok")) {
                return true;
            } else {
                throw new Exception("save failed." + s);
            }
        } finally {
        }
    }

    public Long get(String name) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "getValue");
        CommonTools.writeString(out, name);

        try {
            InputStream in = ch.Commit(net.xinshi.pigeon.netty.common.Constants.ATOM_CODE, out);
            if (in == null) {
                throw new Exception("server error; timeout");
            }
            String state = CommonTools.readString(in);
            if (StringUtils.equals(state, "ok")) {
                String s = CommonTools.readString(in);
                s = StringUtils.trim(s);
                long r = Long.parseLong(s);
                return r;
            } else {
                return null;
            }
        } finally {
        }
    }

    @Override
    public List<Long> getAtoms(List<String> atomIds) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "getAtoms");
        for (String atomId : atomIds) {
            CommonTools.writeString(out, atomId);
        }

        try {
            InputStream in = ch.Commit(net.xinshi.pigeon.netty.common.Constants.ATOM_CODE, out);
            if (in == null) {
                throw new Exception("server error; timeout");
            }
            String state = CommonTools.readString(in);
            if (StringUtils.equals(state, "ok")) {
                List<Long> result = new Vector<Long>();
                String s = CommonTools.readString(in);
                while (s != null) {
                    if (StringUtils.isNumeric(s)) {
                        result.add(Long.parseLong(s));
                    } else {
                        result.add(null);
                    }
                    s = CommonTools.readString(in);

                }
                return result;
            } else {
                throw new Exception("server error." + state);
            }

        } finally {

        }
    }

    public long greaterAndIncReturnLong(String name, int testValue, int incValue)
            throws Exception {

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "greaterAndInc");
        CommonTools.writeString(out, name);
        CommonTools.writeString(out, "" + testValue);
        CommonTools.writeString(out, "" + incValue);

        try {
            InputStream in = ch.Commit(net.xinshi.pigeon.netty.common.Constants.ATOM_CODE, out);
            if (in == null) {
                throw new Exception("server error; timeout");
            }
            String state = CommonTools.readString(in);
            if (StringUtils.equals(state, "ok")) {
                String s = CommonTools.readString(in);
                s = StringUtils.lowerCase(s);
                if (StringUtils.isNumeric(s)) {
                    return Long.parseLong(s);
                } else {
                    throw new Exception("save failed." + s);
                }
            } else {
                throw new Exception("server error." + state);
            }

        } finally {

        }
    }

    @Override
    public boolean greaterAndInc(String name, int testValue, int incValue) throws Exception {
        long rl = greaterAndIncReturnLong(name, testValue, incValue);
        return true;
    }

    @Override
    public boolean lessAndInc(String name, int testValue, int incValue) throws Exception {
        long rl = lessAndIncReturnLong(name, testValue, incValue);
        return true;
    }

    public long lessAndIncReturnLong(String name, int testValue, int incValue) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "lessAndInc");
        CommonTools.writeString(out, name);
        CommonTools.writeString(out, "" + testValue);
        CommonTools.writeString(out, "" + incValue);

        try {
            InputStream in = ch.Commit(net.xinshi.pigeon.netty.common.Constants.ATOM_CODE, out);
            if (in == null) {
                throw new Exception("server error; timeout");
            }
            String state = CommonTools.readString(in);
            if (StringUtils.equals(state, "ok")) {
                String s = CommonTools.readString(in);
                s = StringUtils.trim(s);
                if (StringUtils.isNumeric(s)) {
                    return Long.parseLong(s);
                } else {
                    throw new Exception("save failed." + s);
                }
            } else {
                throw new Exception("server error." + state);
            }

        } finally {

        }
    }

    public void init() throws Exception {
        ch = new Client(host, port, 5);
        boolean rc = ch.init();
        if (!rc) {
            System.out.println("atom init netty client failed!!!!!!!!");
        }
    }

    public void set_state_word(int state_word) throws Exception {

    }
}
