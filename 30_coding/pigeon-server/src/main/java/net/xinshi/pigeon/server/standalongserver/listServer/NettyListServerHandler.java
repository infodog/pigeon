package net.xinshi.pigeon.server.standalongserver.listServer;

import net.xinshi.pigeon.util.CommonTools;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-1-30
 * Time: 下午4:52
 * To change this template use File | Settings | File Templates.
 */

public class NettyListServerHandler {

    static Logger logger = Logger.getLogger("NettyListServerHandler");

    public static ByteArrayOutputStream handle(ListServer listServer, InputStream in) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            String action = CommonTools.readString(in);
            logger.log(Level.FINER, "list connector,action=" + action);
            if (action.equals("getRange")) {
                listServer.doGetRange(in, out);
            } else if (action.equals("delete")) {
                listServer.doDelete(in, out);
            } else if (action.equals("add")) {
                listServer.doAdd(in, out);
            } else if (action.equals("reorder")) {
                listServer.doReorder(in, out);
            } else if (action.equals("isExists")) {
                listServer.doIsExists(in, out);
            } else if (action.equals("getLessOrEqualPos")) {
                listServer.doGetLessOrEqualPos(in, out);
            } else if (action.equals("getSortListObject")) {
                listServer.doGetSortListObject(in, out);
            } else if (action.equals("getSize")) {
                listServer.doGetSize(in, out);
            }
            return out;
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        return null;
    }
}



