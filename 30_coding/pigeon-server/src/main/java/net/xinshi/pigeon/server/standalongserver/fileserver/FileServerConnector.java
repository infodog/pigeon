package net.xinshi.pigeon.server.standalongserver.fileserver;

import net.xinshi.pigeon.util.CommonTools;
import org.apache.http.*;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: mac
 * Date: 11-12-1
 * Time: 下午3:50
 * To change this template use File | Settings | File Templates.
 */
public class FileServerConnector  implements HttpRequestHandler {
    FileServer fileServer;
    Logger logger = Logger.getLogger("FileServerConnector");

    public FileServer getFileServer() {
        return fileServer;
    }

    public void setFileServer(FileServer fileServer) {
        this.fileServer = fileServer;
    }

    @Override
    public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
         if (request instanceof HttpEntityEnclosingRequest) {
            HttpEntity entity = ((HttpEntityEnclosingRequest) request).getEntity();
            final InputStream in = entity.getContent();
            try {
                //String version = CommonTools.readString(in);
                String action = CommonTools.readString(in);
                //String ver_info = flexObjectServer.check_version_action(version, action);
                logger.log(Level.FINER, "action=" + action);
                if (action.equals("upload")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    fileServer.doSaveFile(in ,out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                } else if(action.equals("genRelated")){
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    fileServer.doGenRelated(in ,out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                }
                else if(action.equals("delete")){
                   ByteArrayOutputStream out = new ByteArrayOutputStream();
                    fileServer.doDelete(in ,out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                }
                else {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    CommonTools.writeString(out,"unknown command:" + action);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                    logger.log(Level.SEVERE,"unknown command:" + action);
                }
            } catch (Exception e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                return;
            }
        }
    }
}
