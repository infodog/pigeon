package net.xinshi.pigeon.server.distributedserver.idserver;

import net.xinshi.pigeon.idgenerator.IIDGenerator;
import net.xinshi.pigeon.idgenerator.impl.MysqlIDGenerator;
import net.xinshi.pigeon.server.distributedserver.BaseServer;
import net.xinshi.pigeon.server.distributedserver.util.Tools;
import net.xinshi.pigeon.util.CommonTools;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午5:23
 * To change this template use File | Settings | File Templates.
 */

public class IdServer extends BaseServer {

    MysqlIDGenerator idgenerator;
    final long MAX_ID_COUNT_EACH_TIME = 20000;

    public IIDGenerator getIdgenerator() {
        return idgenerator;
    }

    public void setIdgenerator(MysqlIDGenerator idgenerator) {
        this.idgenerator = idgenerator;
    }

    public Map getStatusMap() {
        return idgenerator.getStatusMap();
    }

    public void doGetNextIds(InputStream in, ByteArrayOutputStream out) throws Exception {
        String idName = CommonTools.readString(in);
        Tools.checkNameLength(idName);
        long count = CommonTools.readLong(in);
        if (count > MAX_ID_COUNT_EACH_TIME) {
            CommonTools.writeString(out, "exceed MAX_ID_COUNT_EACH_TIME:" + count);
            return;
        }
        long from = 0;
        long to = 0;
        from = idgenerator.getIdAndForward(idName, (int) count);
        to = from + count - 1;
        CommonTools.writeString(out, "ok");
        CommonTools.writeLong(out, from);
        CommonTools.writeLong(out, to);
    }

    public void setSkipValue(InputStream in, ByteArrayOutputStream out) throws Exception {
        String idName = CommonTools.readString(in);
        Tools.checkNameLength(idName);
        long count = CommonTools.readLong(in);
        long from = 0;
        long to = 0;
        from = idgenerator.getIdAndForward(idName, (int) count);
        to = from + count - 1;
        CommonTools.writeString(out, "ok");
        CommonTools.writeLong(out, from);
        CommonTools.writeLong(out, to);
    }

    public ByteArrayOutputStream doSyncDataItems(InputStream in) throws Exception {
        while (true) {
            long version = 0L;
            String line;
            try {
                version = CommonTools.readLong(in);
                byte[] data = CommonTools.readBytes(in);
                line = new String(data, "UTF-8");
            } catch (Exception e) {
                break;
            }
            try {
                writes++;
                ((MysqlIDGenerator) getIdgenerator()).writeVersionLogAndCache(version, line);
            } catch (Exception e) {
                throw e;
            }
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "ok");
        return out;
    }

    public ByteArrayOutputStream doPullDataItems(InputStream in) throws Exception {
        while (true) {
            long min = 0L;
            long max = 0L;
            try {
                min = CommonTools.readLong(in);
                max = CommonTools.readLong(in);
                if (min < 1 || max < min) {
                    throw new Exception("doPullDataItems min or max error ... ");
                }
            } catch (Exception e) {
                break;
            }
            try {
                return (((MysqlIDGenerator) getIdgenerator())).pullDataItems(min, max);
            } catch (Exception e) {
                throw e;
            }
        }
        throw new Exception("doPullDataItems error ...... ");
    }

    public ByteArrayOutputStream doCommand(InputStream in) throws Exception {
        try {
            String cmd = CommonTools.readString(in);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            if (cmd.equals("version")) {
                long ver = (((MysqlIDGenerator) getIdgenerator())).verLogger.getVersion();
                CommonTools.writeString(out, String.valueOf(ver));
                return out;
            }
            return null;
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public void stop() {
    }
}


