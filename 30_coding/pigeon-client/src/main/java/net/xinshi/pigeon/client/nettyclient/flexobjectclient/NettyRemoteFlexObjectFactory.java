package net.xinshi.pigeon.client.nettyclient.flexobjectclient;

import net.xinshi.pigeon.flexobject.FlexObjectEntry;
import net.xinshi.pigeon.flexobject.IFlexObjectFactory;
import net.xinshi.pigeon.netty.client.Client;
import net.xinshi.pigeon.util.CommonTools;
import org.apache.commons.lang.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-1-30
 * Time: 上午10:01
 * To change this template use File | Settings | File Templates.
 */

public class NettyRemoteFlexObjectFactory implements IFlexObjectFactory {
    String url;

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

    int sizeToCompress = 512;

    public int getSizeToCompress() {
        return sizeToCompress;
    }

    public void setSizeToCompress(int sizeToCompress) {
        this.sizeToCompress = sizeToCompress;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    Logger logger = Logger.getLogger("NettyRemoteFlexObjectFactory");

    public String getContent(String name) throws Exception {
        FlexObjectEntry entry = getFlexObject(name);
        if (entry == null) {
            return null;
        }
        return entry.getContent();
    }

    public String getConstant(String name) throws Exception {
        throw new Exception("not implement getConstant() ...... ");
    }

    public void saveContent(String name, String content) throws Exception {
        if (content == null) {
            content = "";
        }
        byte[] bytes = content.getBytes("utf-8");
        boolean isCompressed = false;
        if (bytes.length > this.sizeToCompress) {
            // logger.info("zip..." + name);
            bytes = CommonTools.zip(bytes);
            isCompressed = true;
        }
        FlexObjectEntry entry = new FlexObjectEntry();
        entry.setName(name);
        entry.setAdd(false);
        entry.setBytesContent(bytes);
        entry.setCompressed(isCompressed);
        entry.setString(true);
        entry.setHash(0);
        saveFlexObject(entry);
    }

    public List<String> getContents(List<String> names) throws Exception {
        List<FlexObjectEntry> entries = getFlexObjects(names);
        List<String> result = new Vector<String>();
        for (FlexObjectEntry entry : entries) {
            result.add(entry.getContent());
        }
        return result;
    }

    @Override
    public void addContent(String name, String value) throws Exception {
        byte[] bytes = value.getBytes("utf-8");
        boolean isCompressed = false;
        if (bytes.length > this.sizeToCompress) {
            bytes = CommonTools.zip(bytes);
            isCompressed = true;
        }
        FlexObjectEntry entry = new FlexObjectEntry();
        entry.setName(name);
        entry.setAdd(true);
        entry.setBytesContent(bytes);
        entry.setCompressed(isCompressed);
        entry.setString(true);
        entry.setHash(0);
        saveFlexObject(entry);
    }

    @Override
    public void addContent(String name, byte[] bytes) throws Exception {
        boolean isCompressed = false;
        if (bytes.length > this.sizeToCompress) {
            bytes = CommonTools.zip(bytes);
            isCompressed = true;
        }
        FlexObjectEntry entry = new FlexObjectEntry();
        entry.setName(name);
        entry.setAdd(true);
        entry.setBytesContent(bytes);
        entry.setCompressed(isCompressed);
        entry.setString(false);
        entry.setHash(0);
        saveFlexObject(entry);
    }

    @Override
    public void saveFlexObject(FlexObjectEntry entry) throws Exception {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            CommonTools.writeString(out, "saveFlexObject");
            CommonTools.writeEntry(out, entry);
            InputStream in = ch.Commit(net.xinshi.pigeon.netty.common.Constants.FLEXOBJECT_CODE, out);
            if (in == null) {
                throw new Exception("server error; timeout");
            }
            String state = CommonTools.readString(in);
            if (StringUtils.equals("ok", state)) {
                return;
            } else {
                logger.log(Level.SEVERE, state);
                throw new Exception(state);
            }
        } finally {
        }
    }

    @Override
    public void saveBytes(String name, byte[] content) throws Exception {
        boolean isCompressed = false;
        if (content.length > sizeToCompress) {
            content = CommonTools.zip(content);
            isCompressed = true;
        }
        FlexObjectEntry entry = new FlexObjectEntry();
        entry.setName(name);
        entry.setAdd(false);
        entry.setBytesContent(content);
        entry.setCompressed(isCompressed);
        entry.setString(false);
        entry.setHash(0);
        saveFlexObject(entry);
    }

    @Override
    public int deleteContent(String name) throws Exception {
        saveContent(name, "");
        return 0;
    }

    @Override
    public List<FlexObjectEntry> getFlexObjects(List<String> names) throws Exception {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            CommonTools.writeString(out, "getFlexObjects");
            for (String name : names) {
                CommonTools.writeString(out, name);
            }
            List<FlexObjectEntry> result = new Vector<FlexObjectEntry>();
            InputStream in = ch.Commit(net.xinshi.pigeon.netty.common.Constants.FLEXOBJECT_CODE, out);
            if (in == null) {
                throw new Exception("server error; timeout");
            }
            String state = CommonTools.readString(in);
            if (StringUtils.equals("ok", state)) {

                FlexObjectEntry content = CommonTools.readEntry(in);
                while (content != null) {
                    result.add(content);
                    content = CommonTools.readEntry(in);
                }
                return result;
            } else {
                logger.log(Level.SEVERE, "server error,state=" + state);
                throw new Exception(state);
            }
        } finally {
        }
    }

    @Override
    public byte[] getBytes(String name) throws Exception {
        FlexObjectEntry foe = null;
        try {
            foe = getFlexObject(name);
            return foe.getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public FlexObjectEntry getFlexObject(String name) throws SQLException, Exception {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            CommonTools.writeString(out, "getFlexObject");
            CommonTools.writeString(out, name);
            InputStream in = ch.Commit(net.xinshi.pigeon.netty.common.Constants.FLEXOBJECT_CODE, out);
            if (in == null) {
                throw new Exception("server error; timeout");
            }
            String state = CommonTools.readString(in);
            if (StringUtils.equals("ok", state)) {
                FlexObjectEntry content = CommonTools.readEntry(in);
                return content;
            } else {
                return null;
            }
        } finally {
        }
    }

    @Override
    public void saveFlexObjects(List<FlexObjectEntry> objs) throws Exception {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            CommonTools.writeString(out, "saveFlexObjects");
            for (FlexObjectEntry entry : objs) {
                CommonTools.writeEntry(out, entry);
            }
            InputStream in = ch.Commit(net.xinshi.pigeon.netty.common.Constants.FLEXOBJECT_CODE, out);
            if (in == null) {
                throw new Exception("server error; timeout");
            }
            String state = CommonTools.readString(in);
            if (StringUtils.equals("ok", state)) {
                return;
            } else if (StringUtils.equals("TooFast", state)) {
                long dirtySize = CommonTools.readLong(in);
                long sleepTime = dirtySize * objs.size() / 60000 / 2;
                Thread.sleep(sleepTime);
            } else {
                logger.log(Level.SEVERE, state);
                throw new Exception(state);
            }
        } finally {
        }
    }

    public void init() throws Exception {
        ch = new Client(host, port, 10);
        boolean rc = ch.init();
        if (!rc) {
            System.out.println("flexobject init netty client failed!!!!!!!!");
        }
    }

    public void stop() throws Exception {

    }

    public void set_state_word(int state_word) throws Exception {
    }

    @Override
    public void setTlsMode(boolean open) {
    }

    @Override
    public void saveTemporaryContent(String name, String content) throws Exception {
    }

}

