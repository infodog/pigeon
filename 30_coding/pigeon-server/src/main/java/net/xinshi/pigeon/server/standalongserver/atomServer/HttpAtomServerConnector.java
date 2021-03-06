package net.xinshi.pigeon.server.standalongserver.atomServer;

import net.xinshi.pigeon.util.CommonTools;
import org.apache.http.*;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by IntelliJ IDEA.
 * User: zhengxiangyang
 * Date: 11-10-30
 * Time: 上午4:01
 * To change this template use File | Settings | File Templates.
 */
public class HttpAtomServerConnector implements HttpRequestHandler {
    private AtomServer atomServer;

    public void handle(final HttpRequest request, final HttpResponse response, final HttpContext context) throws HttpException, IOException {
        if (request instanceof HttpEntityEnclosingRequest) {
            HttpEntity entity = ((HttpEntityEnclosingRequest) request).getEntity();
            final InputStream in = entity.getContent();
            try {

                String action = CommonTools.readString(in);

                if (action.equals("getValue")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    this.atomServer.doGetValue(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                } else if (action.equals("createAndSet")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    this.atomServer.doCreateAndSet(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                } else if (action.equals("greaterAndInc")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    this.atomServer.doGreaterAndInc(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                } else if (action.equals("lessAndInc")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    this.atomServer.doLessAndInc(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                } else if (action.equals("getAtoms")){
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    this.atomServer.doGetAtoms(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                }
            } catch (Exception e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                return;
            }
        }
    }

    public AtomServer getAtomServer() {
        return atomServer;
    }

    public void setAtomServer(AtomServer atomServer) {
        this.atomServer = atomServer;
    }
}