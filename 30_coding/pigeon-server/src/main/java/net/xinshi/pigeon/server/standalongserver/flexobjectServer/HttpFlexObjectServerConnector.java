package net.xinshi.pigeon.server.standalongserver.flexobjectServer;

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
 * User: zhengxiangyang
 * Date: 11-10-30
 * Time: 上午2:08
 * To change this template use File | Settings | File Templates.
 */
public class HttpFlexObjectServerConnector implements HttpRequestHandler {
    FlexObjectServer flexObjectServer;
    Logger logger = Logger.getLogger("HttpFlexObjectServerConnector");

    public FlexObjectServer getFlexObjectHandler() {
        return flexObjectServer;
    }

    public void setFlexObjectHandler(FlexObjectServer flexObject) {
        this.flexObjectServer = flexObject;
    }

    public void handle(final HttpRequest request, final HttpResponse response, final HttpContext context) throws HttpException, IOException {
        // System.out.println("C:\\work\\pigeon2.0\\30_coding\\pigeonserver\\src\\main\\java\\net\\xinshi\\pigeon\\server\\standalongserver\\flexobjectServer\\HttpFlexObjectServerConnector.java handle");

        if (request instanceof HttpEntityEnclosingRequest) {
            HttpEntity entity = ((HttpEntityEnclosingRequest) request).getEntity();
            final InputStream in = entity.getContent();
            try {
                //String version = CommonTools.readString(in);
                String action = CommonTools.readString(in);
                //String ver_info = flexObjectServer.check_version_action(version, action);
                logger.log(Level.FINER, "action=" + action);
                if (action.equals("getContent")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    flexObjectServer.doGetContent(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                } else if (action.equals("getContents")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    flexObjectServer.doGetContents(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                } else if (action.equals("saveContent")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    flexObjectServer.doSaveContent(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                } else if (action.equals("saveFlexObject")) {
                    //下面3个方法是主力的方法，其他方法都废弃了
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    flexObjectServer.doSaveFlexObject(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                } else if (action.equals("getFlexObjects")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    flexObjectServer.doGetFlexObjects(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                } else if (action.equals("getFlexObject")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    flexObjectServer.doGetFlexObject(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                }  else if (action.equals("saveFlexObjects")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    flexObjectServer.doSaveFlexObjects(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                } else {
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
