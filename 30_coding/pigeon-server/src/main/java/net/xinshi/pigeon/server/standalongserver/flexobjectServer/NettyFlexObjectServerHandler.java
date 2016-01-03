package net.xinshi.pigeon.server.standalongserver.flexobjectServer;

import net.xinshi.pigeon.util.CommonTools;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-1-29
 * Time: 下午3:02
 * To change this template use File | Settings | File Templates.
 */

public class NettyFlexObjectServerHandler {

    static Logger logger = Logger.getLogger("NettyFlexObjectServerHandler");

    public static ByteArrayOutputStream handle(FlexObjectServer flexObjectServer, InputStream in) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            //String version = CommonTools.readString(in);
            String action = CommonTools.readString(in);
            //String ver_info = flexObjectServer.check_version_action(version, action);
            logger.log(Level.FINER, "action=" + action);
            if (action.equals("getContent")) {
                flexObjectServer.doGetContent(in, out);
            } else if (action.equals("getContents")) {
                flexObjectServer.doGetContents(in, out);
            } else if (action.equals("saveContent")) {
                flexObjectServer.doSaveContent(in, out);
            } else if (action.equals("saveFlexObject")) {
                //下面3个方法是主力的方法，其他方法都废弃了
                flexObjectServer.doSaveFlexObject(in, out);
            } else if (action.equals("getFlexObjects")) {
                flexObjectServer.doGetFlexObjects(in, out);
            } else if (action.equals("getFlexObject")) {
                flexObjectServer.doGetFlexObject(in, out);
            } else if (action.equals("saveFlexObjects")) {
                flexObjectServer.doSaveFlexObjects(in, out);
            } else {
                CommonTools.writeString(out, "unknown command:" + action);
                logger.log(Level.SEVERE, "unknown command:" + action);
            }
            return out;
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        return null;
    }
}


