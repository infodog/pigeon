package net.xinshi.pigeon.server.distributedserver.idserver;

import net.xinshi.pigeon.backup.manager.MysqlDump;
import net.xinshi.pigeon.backup.manager.OracleDump;
import net.xinshi.pigeon.distributed.duplicate.DuplicateService;
import net.xinshi.pigeon.idgenerator.impl.MysqlIDGenerator;
import net.xinshi.pigeon.idgenerator.impl.OracleIDGenerator;
import net.xinshi.pigeon.netty.common.Constants;
import net.xinshi.pigeon.server.distributedserver.PigeonServer;
import net.xinshi.pigeon.util.CommonTools;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午5:28
 * To change this template use File | Settings | File Templates.
 */

public class NettyIdServerHandler {

    static Logger logger = Logger.getLogger(NettyIdServerHandler.class.getName());

    public static ByteArrayOutputStream handle(IdServer idServer, InputStream in) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            String action = CommonTools.readString(in);
            if (action.equals("getIdRange")) {
                idServer.doGetNextIds(in, out);
            } else if (action.equals("setSkipValue")) {
                idServer.setSkipValue(in, out);
            } else if (action.equals("backup")) {
                String key = CommonTools.readString(in);
                String uri = "";
                if (idServer.getIdgenerator() instanceof OracleIDGenerator) {
                    uri = OracleDump.dump(Constants.ID_TYPE, key, ((OracleIDGenerator) idServer.getIdgenerator()).tableName, ((OracleIDGenerator) idServer.getIdgenerator()).getDs());
                } else {
                    uri = MysqlDump.dump(Constants.ID_TYPE, key, ((MysqlIDGenerator) idServer.getIdgenerator()).tableName, ((MysqlIDGenerator) idServer.getIdgenerator()).getDs());
                }
                uri = ":" + PigeonServer.httpPort + "/download?filename=" + uri;
                CommonTools.writeString(out, "OK");
                CommonTools.writeString(out, uri);
            } else {
                CommonTools.writeString(out, "error,not implemented method:" + action);
            }
        } catch (Exception e) {
            e.printStackTrace();
            CommonTools.writeString(out, e.getMessage());
        }
        return out;
    }

    public static ByteArrayOutputStream handle(IdServer idServer, InputStream in, int flag) throws Exception {
        if (flag == 0) {
            if (!DuplicateService.isMaster(idServer.getIdgenerator())) {
                return null;
            }
            return handle(idServer, in);
        } else if (flag == 0x80 || flag == 0x40) {
            return idServer.doSyncDataItems(in);
        } else if (flag == 0x20) {
            return idServer.doPullDataItems(in);
        } else if (flag == 0xF0) {
            return idServer.doCommand(in);
        }
        return null;
    }

}



