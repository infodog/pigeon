package net.xinshi.pigeon.client.httpclient.idclient;

import net.xinshi.pigeon.idgenerator.IIDGenerator;
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
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by IntelliJ IDEA.
 * User: mac
 * Date: 11-11-13
 * Time: 上午12:54
 * To change this template use File | Settings | File Templates.
 */
public class HttpIdGenerator implements IIDGenerator {
    HttpClient httpClient;
    long idNumPerRound;
    String url;

    public String getUrl() {
        return url;
    }

    public long getIdNumPerRound() {
        return idNumPerRound;
    }

    public void setIdNumPerRound(long idNumPerRound) {
        this.idNumPerRound = idNumPerRound;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    class IdPair {
        public IdPair() {
            curVal = 0;
            maxVal = 0;
        }

        public int curVal;
        public int maxVal;
    }


    ConcurrentHashMap Ids = null;

    @Override
    public long getId(String name) throws Exception {
        int idForThisTime;
        if (Ids == null) {
            Ids = new ConcurrentHashMap();
        }
        IdPair id = (IdPair) Ids.get(name);


        if (id != null && id.curVal <= id.maxVal) {
            idForThisTime = id.curVal++;
            return idForThisTime;
        }


        id = new IdPair();


        HttpPost httpPost = new HttpPost(url);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "getIdRange");
        CommonTools.writeString(out, name);
        CommonTools.writeLong(out, idNumPerRound);


        httpPost.setEntity(new ByteArrayEntity(out.toByteArray()));

        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();
        try {

            if (entity != null) {
                InputStream in = entity.getContent();
                String state = CommonTools.readString(in);
                if (StringUtils.equals("ok", state)) {
                    long from = CommonTools.readLong(in);
                    long to = CommonTools.readLong(in);
                    id.curVal = (int) from;
                    id.maxVal = (int) to;
                    long c = id.curVal++;
                    Ids.put(name, id);
                    return c;

                } else {
                    throw new Exception("server error, state=" + state);
                }
            }
            throw new Exception("Connection reset");
        } finally {
            if (entity != null) {
                entity.consumeContent();
            }
        }

    }

    public long setSkipValue(String name, long value) throws Exception {
        throw new Exception("method setSkipValue(...) forbidden!");
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
        Ids = new ConcurrentHashMap();


    }

    public void set_state_word(int state_word) throws Exception {
    }

}
