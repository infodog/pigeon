package net.xinshi.pigeon.client.httpclient.flexobjectclient;

import net.xinshi.pigeon.flexobject.FlexObjectEntry;
import net.xinshi.pigeon.flexobject.IFlexObjectFactory;
import net.xinshi.pigeon.util.CommonTools;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.params.ConnManagerParams;
import org.apache.http.conn.params.ConnPerRouteBean;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: zhengxiangyang
 * Date: 11-11-1
 * Time: 上午10:36
 * To change this template use File | Settings | File Templates.
 */
public class HttpRemoteFlexObjectFactory implements IFlexObjectFactory {
    HttpClient httpClient;
    String url;

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

    Logger logger = Logger.getLogger("HttpRemoteFlexObjectFactory");


    public String getContent(String name) throws Exception {
        FlexObjectEntry entry = getFlexObject(name);
        if (entry == null) {
            return null;
        }
        return entry.getContent();

    }

    public String getConstant(String name) throws Exception {
        throw  new Exception("not implement getConstant() ...... ");
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
        HttpEntity entity = null;
        try {
            HttpPost httpPost = new HttpPost(url);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            CommonTools.writeString(out, "saveFlexObject");
            CommonTools.writeEntry(out, entry);


            httpPost.setEntity(new ByteArrayEntity(out.toByteArray()));

            HttpResponse response = httpClient.execute(httpPost);
            entity = response.getEntity();

            if (entity != null) {
                InputStream in = entity.getContent();
                String state = CommonTools.readString(in);

                if (StringUtils.equals("ok", state)) {
                    return;
                } else {
                    logger.log(Level.SEVERE, state);
                    throw new Exception(state);
                }

            } else {
                logger.log(Level.SEVERE, "server error; server reset");
                throw new Exception("server error; server reset");
            }
        } finally {
            if (entity != null) {
                entity.consumeContent();
            }
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
        HttpEntity entity = null;
        try {
            HttpPost httpPost = new HttpPost(url);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            CommonTools.writeString(out, "getFlexObjects");
            for (String name : names) {
                CommonTools.writeString(out, name);
            }
            List<FlexObjectEntry> result = new Vector<FlexObjectEntry>();

            httpPost.setEntity(new ByteArrayEntity(out.toByteArray()));

            HttpResponse response = httpClient.execute(httpPost);
            entity = response.getEntity();

            if (entity != null) {
                InputStream in = entity.getContent();
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

            } else {
                logger.log(Level.SEVERE, "server reset");
                throw new Exception("server reset");
            }
        } finally {
            if (entity != null) {
                entity.consumeContent();
            }
        }
    }

    @Override
    public FlexObjectEntry getFlexObject(String name) throws SQLException, Exception {
        HttpEntity entity = null;
        try {
            HttpPost httpPost = new HttpPost(url);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            CommonTools.writeString(out, "getFlexObject");
            CommonTools.writeString(out, name);


            httpPost.setEntity(new ByteArrayEntity(out.toByteArray()));

            HttpResponse response = httpClient.execute(httpPost);
            entity = response.getEntity();

            if (entity != null) {
                InputStream in = entity.getContent();
                String state = CommonTools.readString(in);

                if (StringUtils.equals("ok", state)) {
                    FlexObjectEntry content = CommonTools.readEntry(in);
                    return content;
                } else {
                    return null;
                }

            } else {
                return null;
            }
        } finally {
            if (entity != null) {
                entity.consumeContent();
            }
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
    public void saveFlexObjects(List<FlexObjectEntry> objs) throws Exception {
        HttpEntity entity = null;
        try {
            HttpPost httpPost = new HttpPost(url);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            CommonTools.writeString(out, "saveFlexObjects");
            for (FlexObjectEntry entry : objs) {
                CommonTools.writeEntry(out, entry);
            }


            httpPost.setEntity(new ByteArrayEntity(out.toByteArray()));

            HttpResponse response = httpClient.execute(httpPost);
            entity = response.getEntity();

            if (entity != null) {
                InputStream in = entity.getContent();
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

            } else {
                logger.log(Level.SEVERE, "server error; server reset");
                throw new Exception("server error; server reset");
            }
        } finally {
            if (entity != null) {
                entity.consumeContent();
            }
        }
    }

    public void init() throws Exception {
        // Create and initialize HTTP parameters
        HttpParams params = new BasicHttpParams();
        ConnManagerParams.setMaxTotalConnections(params, 100);
        ConnManagerParams.setMaxConnectionsPerRoute(params, new ConnPerRouteBean(20));
        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);

        // Create and initialize scheme registry
        SchemeRegistry schemeRegistry = new SchemeRegistry();
        schemeRegistry.register(new Scheme("http", PlainSocketFactory.getSocketFactory(), 80));

        // Create an HttpClient with the ThreadSafeClientConnManager.
        // This connection manager must be used if more than one thread will
        // be using the HttpClient.
        ClientConnectionManager cm = new ThreadSafeClientConnManager(params, schemeRegistry);
        httpClient = new DefaultHttpClient(cm, params);

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
