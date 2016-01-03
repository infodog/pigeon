package net.xinshi.pigeon.backup.util;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.params.ConnManagerParams;
import org.apache.http.conn.params.ConnPerRouteBean;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-9-7
 * Time: 上午10:51
 * To change this template use File | Settings | File Templates.
 */

public class DownloadAppendToFile {
    static HttpClient downloadHttpClient = null;

    static {
        HttpParams params = new BasicHttpParams();
        ConnManagerParams.setMaxTotalConnections(params, 100);
        ConnManagerParams.setMaxConnectionsPerRoute(params, new ConnPerRouteBean(30));
        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
        SchemeRegistry schemeRegistry = new SchemeRegistry();
        schemeRegistry.register(new Scheme("http", PlainSocketFactory.getSocketFactory(), 80));
        ClientConnectionManager cm = new ThreadSafeClientConnManager(params, schemeRegistry);
        downloadHttpClient = new DefaultHttpClient(cm, params);
    }

    public static void downloadAppendToFile(String filename, String url) throws Exception {
        File f = new File(filename);
        FileOutputStream fos = new FileOutputStream(f, true);
        HttpEntity entity = null;
        String hash = new String(url);
        hash = hash.replace("\t", "").replace(" ", "").replace("\r", "").replace("\n", "");
        hash += "\r\n";
        fos.write(hash.getBytes());
        try {

            HttpGet get = new HttpGet(url);
            HttpResponse response = downloadHttpClient.execute(get);
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new Exception("response.getStatusLine().getStatusCode() != 200");
            }
            entity = response.getEntity();
            InputStream in = entity.getContent();
            byte[] buf = new byte[2048];
            int n = in.read(buf);
            while (n > 0) {
                fos.write(buf, 0, n);
                n = in.read(buf);
            }
        } finally {
            if (fos != null) {
                fos.close();
            }
            if (entity != null) {
                entity.consumeContent();
            }
        }
    }
}

