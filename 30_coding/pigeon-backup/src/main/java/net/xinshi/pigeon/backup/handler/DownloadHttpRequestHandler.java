package net.xinshi.pigeon.backup.handler;

import net.xinshi.pigeon.backup.util.BackupTools;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;

import java.io.File;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-9-6
 * Time: 下午3:15
 * To change this template use File | Settings | File Templates.
 */

public class DownloadHttpRequestHandler implements HttpRequestHandler {

    public void handle(HttpRequest httpRequest, HttpResponse httpResponse, HttpContext httpContext) throws HttpException, IOException {
        String prefix = "/download?filename=";
        String filename = httpRequest.getRequestLine().getUri();
        if (!filename.startsWith(prefix)) {
            httpResponse.setStatusCode(404);
            httpResponse.setEntity(new StringEntity("?filename=abc.txt"));
        } else {
            filename = filename.substring(prefix.length());
            File f = new File(BackupTools.getBackupPath() + "/" + filename);
            FileEntity fe = new FileEntity(f, "text/plain");
            httpResponse.setEntity(fe);
        }
    }

}

