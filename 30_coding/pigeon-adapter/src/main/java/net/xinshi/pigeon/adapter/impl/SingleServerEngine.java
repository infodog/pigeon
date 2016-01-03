package net.xinshi.pigeon.adapter.impl;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.atom.IIntegerAtom;
import net.xinshi.pigeon.client.httpclient.atomclient.HttpAtom;
import net.xinshi.pigeon.client.httpclient.flexobjectclient.HttpRemoteFlexObjectFactory;
import net.xinshi.pigeon.client.httpclient.idclient.HttpIdGenerator;
import net.xinshi.pigeon.client.httpclient.listclient.HttpListFactory;
import net.xinshi.pigeon.client.minaclient.AsynLockClient;
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
 * User: mac
 * Date: 11-11-5
 * Time: 下午4:58
 * To change this template use File | Settings | File Templates.
 */
public class SingleServerEngine implements IPigeonStoreEngine {

    HttpRemoteFlexObjectFactory flexobjectFactory;
    HttpListFactory listFactory;
    HttpAtom atom;
    AsynLockClient lock;
    HttpIdGenerator idGenerator;

    Logger logger = Logger.getLogger("SingleServerEngine");

    public IResourceLock getLock() {
        return lock;
    }

    @Override
    public void stop() throws InterruptedException {
        if (lock != null) {
            lock.stop();
        }
    }

    public SingleServerEngine(String configFile) throws Exception {
        initFromFile(configFile);

    }

    public SingleServerEngine(String path, boolean asResource) throws Exception {

        logger.log(Level.INFO, "Path=" + path + " asResource=" + asResource);
        if (asResource == true) {
            initFromResource(path);
        } else {
            initFromFile(path);
        }
    }


    public SingleServerEngine(JSONObject config) throws Exception {
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
        flexobjectFactory = new HttpRemoteFlexObjectFactory();
        flexobjectFactory.setUrl(baseUrl + config.getString("flexobject"));
        flexobjectFactory.init();

        listFactory = new HttpListFactory();
        listFactory.setUrl(baseUrl + config.getString("list"));
        listFactory.init();

        atom = new HttpAtom();
        atom.setUrl(baseUrl + config.getString("atom"));
        atom.init();

        String lockhost = config.optString("lockhost");
        String lockport = config.optString("lockport");
        if (StringUtils.isNotBlank(lockhost) && StringUtils.isNotBlank(lockport)) {
            lock = new AsynLockClient(config.getString("lockhost"), config.getString("lockport"));
        }
        idGenerator = new HttpIdGenerator();
        idGenerator.setIdNumPerRound(config.getLong("idNumPerRound"));
        idGenerator.setUrl(config.getString("idserverUrl"));
        idGenerator.init();


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
