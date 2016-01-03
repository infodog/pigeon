package net.xinshi.pigeon.server.standalongserver.atomServer;

import net.xinshi.pigeon.util.CommonTools;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-1-31
 * Time: 下午2:20
 * To change this template use File | Settings | File Templates.
 */

public class NettyAtomServerHandler {

    static Logger logger = Logger.getLogger("NettyAtomServerHandler");

    public static ByteArrayOutputStream handle(AtomServer atomServer, InputStream in) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            String action = CommonTools.readString(in);

            if (action.equals("getValue")) {
                atomServer.doGetValue(in, out);
            } else if (action.equals("createAndSet")) {
                atomServer.doCreateAndSet(in, out);
            } else if (action.equals("greaterAndInc")) {
                atomServer.doGreaterAndInc(in, out);
            } else if (action.equals("lessAndInc")) {
                atomServer.doLessAndInc(in, out);
            } else if (action.equals("getAtoms")) {
                atomServer.doGetAtoms(in, out);
            }
            return out;
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        return null;
    }
}
