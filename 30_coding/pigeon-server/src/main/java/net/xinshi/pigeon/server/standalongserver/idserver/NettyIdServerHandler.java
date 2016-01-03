package net.xinshi.pigeon.server.standalongserver.idserver;

import net.xinshi.pigeon.util.CommonTools;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-1-31
 * Time: 下午2:27
 * To change this template use File | Settings | File Templates.
 */

public class NettyIdServerHandler {

    static Logger logger = Logger.getLogger("NettyIdServerHandler");

    public static ByteArrayOutputStream handle(IdServer idServer, InputStream in) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            String action = CommonTools.readString(in);
            if (action.equals("getIdRange")) {
                idServer.doGetNextIds(in, out);
            } else {
                CommonTools.writeString(out, "error,not implemented method:" + action);
            }
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            CommonTools.writeString(out, e.getMessage());
        }
        return out;
    }
}



