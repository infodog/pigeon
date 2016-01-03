package net.xinshi.pigeon.client.httpclient.atomclient;

import net.xinshi.pigeon.atom.IIntegerAtom;
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
import java.util.List;
import java.util.Vector;

/**
 * Created by IntelliJ IDEA.
 * User: zhengxiangyang
 * Date: 11-11-1
 * Time: 上午11:00
 * To change this template use File | Settings | File Templates.
 */
public class HttpAtom implements IIntegerAtom {
    HttpClient httpClient;
    String url;

    public boolean createAndSet(String name, Integer initValue)
            throws Exception {
        HttpPost httpPost = new HttpPost(url);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "createAndSet");
        CommonTools.writeString(out, name);
        CommonTools.writeString(out, "" + initValue);


        httpPost.setEntity(new ByteArrayEntity(out.toByteArray()));

        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();
        try {

            if (entity != null) {
                InputStream in = entity.getContent();
                String s = CommonTools.readString(in);
                s = StringUtils.trim(s);
                if (s.equals("ok")) {
                    return true;
                } else {
                    throw new Exception("save failed." + s);
                }
            }
            return false;
        } finally {
            if (entity != null) {
                entity.consumeContent();
            }
        }
    }

    public Long get(String name) throws Exception {
        HttpPost httpPost = new HttpPost(url);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "getValue");
        CommonTools.writeString(out, name);

        httpPost.setEntity(new ByteArrayEntity(out.toByteArray()));

        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();
        try {
            if (entity != null) {
                InputStream in = entity.getContent();
                String state = CommonTools.readString(in);
                if (StringUtils.equals(state, "ok")) {
                    String s = CommonTools.readString(in);
                    s = StringUtils.trim(s);
                    long r = Long.parseLong(s);
                    return r;
                } else {
                    return null;
                }
            }
        } finally {
            if (entity != null) {
                entity.consumeContent();
            }
        }
        return null;
    }

    @Override
    public List<Long> getAtoms(List<String> atomIds) throws Exception {
        HttpPost httpPost = new HttpPost(url);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "getAtoms");
        for (String atomId : atomIds) {
            CommonTools.writeString(out, atomId);
        }


        httpPost.setEntity(new ByteArrayEntity(out.toByteArray()));

        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();

        try {
            if (entity != null) {
                InputStream in = entity.getContent();
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
            }
            throw new Exception("server error.Connection reset");
        } finally {
            if (entity != null) {
                entity.consumeContent();
            }
        }
    }

    public long greaterAndIncReturnLong(String name, int testValue, int incValue)
            throws Exception {
        HttpPost httpPost = new HttpPost(url);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "greaterAndInc");
        CommonTools.writeString(out, name);
        CommonTools.writeString(out, "" + testValue);
        CommonTools.writeString(out, "" + incValue);


        httpPost.setEntity(new ByteArrayEntity(out.toByteArray()));

        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();

        try {
            if (entity != null) {
                InputStream in = entity.getContent();
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
            }
            throw new Exception("server error.Connection reset");
        } finally {
            if (entity != null) {
                entity.consumeContent();
            }
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
        HttpPost httpPost = new HttpPost(url);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "lessAndInc");
        CommonTools.writeString(out, name);
        CommonTools.writeString(out, "" + testValue);
        CommonTools.writeString(out, "" + incValue);


        httpPost.setEntity(new ByteArrayEntity(out.toByteArray()));

        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();
        try {
            if (entity != null) {
                InputStream in = entity.getContent();
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
            }
            throw new Exception("server error.Connection reset");
        } finally {
            if (entity != null) {
                entity.consumeContent();
            }
        }
    }


    public void init() throws Exception {
        HttpParams params = new BasicHttpParams();
        ConnManagerParams.setMaxTotalConnections(params, 100);
        ConnManagerParams.setMaxConnectionsPerRoute(params, new ConnPerRouteBean(20));
        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);

        SchemeRegistry schemeRegistry = new SchemeRegistry();
        schemeRegistry.register(new Scheme("http", PlainSocketFactory.getSocketFactory(), 80));

        ClientConnectionManager cm = new ThreadSafeClientConnManager(params, schemeRegistry);
        httpClient = new DefaultHttpClient(cm, params);
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void set_state_word(int state_word) throws Exception {

    }
}