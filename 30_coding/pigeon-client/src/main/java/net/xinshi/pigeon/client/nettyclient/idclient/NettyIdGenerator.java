package net.xinshi.pigeon.client.nettyclient.idclient;

import net.xinshi.pigeon.idgenerator.IIDGenerator;
import net.xinshi.pigeon.netty.client.Client;
import net.xinshi.pigeon.util.CommonTools;
import org.apache.commons.lang.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-1-31
 * Time: 下午2:49
 * To change this template use File | Settings | File Templates.
 */

public class NettyIdGenerator implements IIDGenerator {
    String host = "127.0.0.1";

    int port = 8878;

    Client ch = null;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Client getCh() {
        return ch;
    }

    public void setCh(Client ch) {
        this.ch = ch;
    }

    long idNumPerRound;
    String url;

    public String getUrl() {
        return url;
    }

    public long getIdNumPerRound() {
        return idNumPerRound;
    }

    public void setIdNumPerRound(long idNumPerRound) {
        this.idNumPerRound = idNumPerRound;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    class IdPair {
        public IdPair() {
            curVal = 0;
            maxVal = 0;
        }

        public int curVal;
        public int maxVal;
    }


    ConcurrentHashMap Ids = null;


    @Override
    public long getId(String name) throws Exception {
        int idForThisTime;
        if (Ids == null) {
            Ids = new ConcurrentHashMap();
        }
        IdPair id = (IdPair) Ids.get(name);


        if (id != null && id.curVal <= id.maxVal) {
            idForThisTime = id.curVal++;
            return idForThisTime;
        }


        id = new IdPair();


        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "getIdRange");
        CommonTools.writeString(out, name);
        CommonTools.writeLong(out, idNumPerRound);


        try {

            InputStream in = ch.Commit(net.xinshi.pigeon.netty.common.Constants.ID_CODE, out);
            if (in == null) {
                throw new Exception("server error; timeout");
            }
            String state = CommonTools.readString(in);
            if (StringUtils.equals("ok", state)) {
                long from = CommonTools.readLong(in);
                long to = CommonTools.readLong(in);
                id.curVal = (int) from;
                id.maxVal = (int) to;
                long c = id.curVal++;
                Ids.put(name, id);
                return c;

            } else {
                throw new Exception("server error, state=" + state);
            }

        } finally {

        }

    }

    public long setSkipValue(String name, long value) throws Exception {
        throw new Exception("method setSkipValue(...) forbidden!");
    }

    public void init() throws Exception {
        Ids = new ConcurrentHashMap();

        ch = new Client(host, port, 5);
        boolean rc = ch.init();
        if (!rc) {
            System.out.println("id init netty client failed!!!!!!!!");
        }
    }

    public void set_state_word(int state_word) throws Exception {
    }
}

