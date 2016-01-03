package net.xinshi.pigeon.server.distributedserver.flexobjectserver;

import net.xinshi.pigeon.backup.manager.MysqlDump;
import net.xinshi.pigeon.backup.manager.OracleDump;
import net.xinshi.pigeon.distributed.duplicate.DuplicateService;
import net.xinshi.pigeon.flexobject.impls.fastsimple.CommonFlexObjectFactory;
import net.xinshi.pigeon.flexobject.impls.fastsimple.OracleFlexObjectFactory;
import net.xinshi.pigeon.netty.common.Constants;
import net.xinshi.pigeon.server.distributedserver.PigeonServer;
import net.xinshi.pigeon.util.CommonTools;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午5:17
 * To change this template use File | Settings | File Templates.
 */

public class NettyFlexObjectServerHandler {

    static Logger logger = Logger.getLogger(NettyFlexObjectServerHandler.class.getName());

    public static ByteArrayOutputStream handle(FlexObjectServer flexObjectServer, InputStream in) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            String action = CommonTools.readString(in);
            if (action.equals("getContent")) {
                flexObjectServer.doGetContent(in, out);
            } else if (action.equals("getContents")) {
                flexObjectServer.doGetContents(in, out);
            } else if (action.equals("saveContent")) {
                flexObjectServer.doSaveContent(in, out);
            } else if (action.equals("saveFlexObject")) {
                flexObjectServer.doSaveFlexObject(in, out);
            } else if (action.equals("getFlexObjects")) {
                flexObjectServer.doGetFlexObjects(in, out);
            } else if (action.equals("getFlexObject")) {
                flexObjectServer.doGetFlexObject(in, out);
            } else if (action.equals("saveFlexObjects")) {
                flexObjectServer.doSaveFlexObjects(in, out);
            } else if (action.equals("backup")) {
                String key = CommonTools.readString(in);
                String uri = "";
                if (flexObjectServer.getFlexObjectFactory() instanceof OracleFlexObjectFactory) {
                    uri = OracleDump.dump(Constants.FLEXOBJECT_TYPE, key, ((OracleFlexObjectFactory) flexObjectServer.getFlexObjectFactory()).getTableName(), ((OracleFlexObjectFactory) flexObjectServer.getFlexObjectFactory()).getDs());
                } else if (flexObjectServer.getFlexObjectFactory() instanceof CommonFlexObjectFactory) {
                    uri = MysqlDump.dump(Constants.FLEXOBJECT_TYPE, key, ((CommonFlexObjectFactory) flexObjectServer.getFlexObjectFactory()).getTableName(), ((CommonFlexObjectFactory) flexObjectServer.getFlexObjectFactory()).getDs());
                }
                uri = ":" + PigeonServer.httpPort + "/download?filename=" + uri;
                CommonTools.writeString(out, "OK");
                CommonTools.writeString(out, uri);
            } else {
                CommonTools.writeString(out, "unknown command:" + action);
                logger.log(Level.SEVERE, "unknown command:" + action);
            }
            return out;
        } catch (Exception e) {
            //e.printStackTrace();
            logger.info(e.getMessage());
            CommonTools.writeString(out, e.getMessage());
        }
        return null;
    }

    public static ByteArrayOutputStream handle(FlexObjectServer flexObjectServer, InputStream in, int flag) throws Exception {
        if (flag == 0) {
            if (!DuplicateService.isMaster(flexObjectServer.getFlexObjectFactory())) {
                return null;
            }
            return handle(flexObjectServer, in);
        } else if (flag == 0x80 || flag == 0x40) {
            return flexObjectServer.doSyncDataItems(in);
        } else if (flag == 0x20) {
            return flexObjectServer.doPullDataItems(in);
        } else if (flag == 0xF0) {
            return flexObjectServer.doCommand(in);
        }
        return null;
    }

}

