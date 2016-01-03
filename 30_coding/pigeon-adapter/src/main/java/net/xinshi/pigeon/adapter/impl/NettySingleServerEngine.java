package net.xinshi.pigeon.adapter.impl;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.atom.IIntegerAtom;
import net.xinshi.pigeon.client.minaclient.AsynLockClient;
import net.xinshi.pigeon.client.nettyclient.atomclient.NettyAtom;
import net.xinshi.pigeon.client.nettyclient.flexobjectclient.NettyRemoteFlexObjectFactory;
import net.xinshi.pigeon.client.nettyclient.idclient.NettyIdGenerator;
import net.xinshi.pigeon.client.nettyclient.listclient.NettyListFactory;
import net.xinshi.pigeon.flexobject.IFlexObjectFactory;
import net.xinshi.pigeon.idgenerator.IIDGenerator;
import net.xinshi.pigeon.list.IListFactory;
import net.xinshi.pigeon.resourcelock.IResourceLock;
import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-1-31
 * Time: 上午11:52
 * To change this template use File | Settings | File Templates.
 */

public class NettySingleServerEngine implements IPigeonStoreEngine {

    NettyRemoteFlexObjectFactory flexobjectFactory;
    NettyListFactory listFactory;
    NettyAtom atom;
    NettyIdGenerator idGenerator;

    AsynLockClient lock;

    Logger logger = Logger.getLogger("NettySingleServerEngine");

    public IResourceLock getLock() {
        return lock;
    }

    @Override
    public void stop() throws InterruptedException {
        if (lock != null) {
            lock.stop();
        }
    }

    public NettySingleServerEngine(String configFile) throws Exception {
        initFromFile(configFile);

    }

    public NettySingleServerEngine(String path, boolean asResource) throws Exception {

        logger.log(Level.INFO, "Path=" + path + " asResource=" + asResource);
        if (asResource == true) {
            initFromResource(path);
        } else {
            initFromFile(path);
        }
    }


    public NettySingleServerEngine(JSONObject config) throws Exception {
        init(config);
    }

    void initFromResource(String path) throws Exception {
        InputStream in = getClass().getResourceAsStream(path);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] buf = new byte[2048];
        int n = in.read(buf);
        while (n > 0) {
            bos.write(buf, 0, n);
            n = in.read(buf);
        }
        in.close();
        String s = new String(bos.toByteArray(), "UTF-8");
        s = StringUtils.trim(s);
        JSONObject jo = new JSONObject(s);
        init(jo);
    }

    void initFromFile(String configFile) throws Exception {
        File f = new File(configFile);
        File file = new File(f.getAbsolutePath());

        byte[] b = new byte[(int) file.length()];
        FileInputStream is = new FileInputStream(f.getAbsolutePath());
        is.read(b);

        is.close();

        String s = new String(b, "UTF-8");
        s = StringUtils.trim(s);
        JSONObject jo = new JSONObject(s);
        init(jo);
    }

    void init(JSONObject config) throws Exception {
        String baseUrl = config.getString("baseurl");
        if (!baseUrl.endsWith("/")) {
            baseUrl += "/";
        }
        String host = config.optString("host");
        int port = config.optInt("port");
        flexobjectFactory = new NettyRemoteFlexObjectFactory();
        flexobjectFactory.setHost(host);
        flexobjectFactory.setPort(port);
        flexobjectFactory.init();

        listFactory = new NettyListFactory();
        // listFactory.setUrl(baseUrl + config.getString("list"));
        listFactory.setHost(host);
        listFactory.setPort(port);
        listFactory.init();

        if (config.optString("atom").length() > 0) {
            atom = new NettyAtom();
            // atom.setUrl(baseUrl + config.getString("atom"));
            atom.setHost(host);
            atom.setPort(port);
            atom.init();
        }

        String lockhost = config.optString("lockhost");
        String lockport = config.optString("lockport");
        if (lockport.length() > 0) {
            if (StringUtils.isNotBlank(lockhost) && StringUtils.isNotBlank(lockport)) {
                lock = new AsynLockClient(config.getString("lockhost"), config.getString("lockport"));
            }
        }

        if (config.getString("idserverUrl").length() > 0) {
            idGenerator = new NettyIdGenerator();
            idGenerator.setIdNumPerRound(config.getLong("idNumPerRound"));
            idGenerator.setUrl(config.getString("idserverUrl"));
            idGenerator.setHost(host);
            idGenerator.setPort(port);
            idGenerator.init();
        }

    }

    @Override
    public IFlexObjectFactory getFlexObjectFactory() {
        return flexobjectFactory;
    }

    @Override
    public IIntegerAtom getAtom() {
        return atom;
    }

    @Override
    public IListFactory getListFactory() {
        return listFactory;
    }

    @Override
    public IIDGenerator getIdGenerator() {
        return idGenerator;
    }
}

