package net.xinshi.pigeon.server.standalongserver.idserver;

import net.xinshi.pigeon.idgenerator.IIDGenerator;
import net.xinshi.pigeon.idgenerator.impl.MysqlIDGenerator;
import net.xinshi.pigeon.server.standalongserver.BaseServer;
import net.xinshi.pigeon.util.CommonTools;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

/**
 * Created by IntelliJ IDEA.
 * User: mac
 * Date: 11-11-12
 * Time: 下午5:31
 * To change this template use File | Settings | File Templates.
 */
public class IdServer extends BaseServer {
    MysqlIDGenerator idgenerator;
    final long MAX_ID_COUNT_EACH_TIME = 100000;

    public IIDGenerator getIdgenerator() {
        return idgenerator;
    }

    public void setIdgenerator(MysqlIDGenerator idgenerator) {
        this.idgenerator = idgenerator;
    }

    public void doGetNextIds(InputStream in, ByteArrayOutputStream out) throws Exception {
        String idName = CommonTools.readString(in);
        long count = CommonTools.readLong(in);
        if (count > MAX_ID_COUNT_EACH_TIME) {
            CommonTools.writeString(out, "exceed MAX_ID_COUNT_EACH_TIME:" + count);
            return;
        }
        long from = 0;
        long to = 0;

        from = idgenerator.getIdAndForward(idName,(int)count);
        to = from + count - 1;
        CommonTools.writeString(out, "ok");
        CommonTools.writeLong(out, from);
        CommonTools.writeLong(out, to);
    }

    @Override
    public void stop() {

    }
}
