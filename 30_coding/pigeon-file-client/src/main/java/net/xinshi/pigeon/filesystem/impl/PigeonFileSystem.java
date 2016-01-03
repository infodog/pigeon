package net.xinshi.pigeon.filesystem.impl;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.adapter.impl.NettySingleServerEngine;
import net.xinshi.pigeon.adapter.impl.SingleServerEngine;
import net.xinshi.pigeon.atom.IIntegerAtom;
import net.xinshi.pigeon.filesystem.IFileSystem;
import net.xinshi.pigeon.flexobject.IFlexObjectFactory;
import net.xinshi.pigeon.idgenerator.IIDGenerator;
import net.xinshi.pigeon.list.IListFactory;
import net.xinshi.pigeon.list.ISortList;
import net.xinshi.pigeon.list.SortListObject;
import net.xinshi.pigeon.util.CommonTools;
import net.xinshi.pigeon.util.HashAlgorithms;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.params.ConnManagerParams;
import org.apache.http.conn.params.ConnPerRouteBean;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.json.JSONObject;

import java.io.*;
import java.util.Calendar;
import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: mac
 * Date: 11-11-3
 * Time: 下午6:46
 * To change this template use File | Settings | File Templates.
 */
public class PigeonFileSystem implements IFileSystem {
    PigeonFileSystemConfig config;
    IFlexObjectFactory flexObjectFactory;
    IIntegerAtom atom;
    IIDGenerator idGenerator;
    IListFactory listFactory;
    Logger logger = Logger.getLogger("PigeonFileSystem");

    public PigeonFileSystem(IPigeonStoreEngine pigeonStore, String fileSystemConfigFile) throws Exception {
        flexObjectFactory = pigeonStore.getFlexObjectFactory();
        atom = pigeonStore.getAtom();
        listFactory = pigeonStore.getListFactory();
        idGenerator = pigeonStore.getIdGenerator();

        config = new PigeonFileSystemConfig(fileSystemConfigFile);
        init();
    }

    public PigeonFileSystem(String pigeonStoreConfigFile, boolean UseNetty, String fileSystemConfigFile) throws Exception {
        IPigeonStoreEngine pigeonStore = null;
        if (UseNetty) {
            pigeonStore = new NettySingleServerEngine(pigeonStoreConfigFile);
        } else {
            pigeonStore = new SingleServerEngine(pigeonStoreConfigFile);
        }
        flexObjectFactory = pigeonStore.getFlexObjectFactory();
        atom = pigeonStore.getAtom();
        listFactory = pigeonStore.getListFactory();
        idGenerator = pigeonStore.getIdGenerator();

        config = new PigeonFileSystemConfig(fileSystemConfigFile);
        init();
    }

    public PigeonFileSystem(String pigeonStoreConfigFile, String fileSystemConfigFile) throws Exception {
        IPigeonStoreEngine pigeonStore = new SingleServerEngine(pigeonStoreConfigFile);
        flexObjectFactory = pigeonStore.getFlexObjectFactory();
        atom = pigeonStore.getAtom();
        listFactory = pigeonStore.getListFactory();
        idGenerator = pigeonStore.getIdGenerator();

        config = new PigeonFileSystemConfig(fileSystemConfigFile);
        init();
    }


    public PigeonFileSystem(String pigeonStoreConfigFile, String fileSystemConfigFile, boolean asResource) throws Exception {
        IPigeonStoreEngine pigeonStore = new SingleServerEngine(pigeonStoreConfigFile, asResource);
        flexObjectFactory = pigeonStore.getFlexObjectFactory();
        atom = pigeonStore.getAtom();
        listFactory = pigeonStore.getListFactory();
        idGenerator = pigeonStore.getIdGenerator();
        config = new PigeonFileSystemConfig(fileSystemConfigFile, asResource);
        init();
    }

    public IListFactory getListFactory() {
        return listFactory;
    }


    public void setListFactory(IListFactory listFactory) {
        this.listFactory = listFactory;
    }

    public IIDGenerator getIdGenerator() {
        return idGenerator;
    }

    public void setIdGenerator(IIDGenerator idGenerator) {
        this.idGenerator = idGenerator;
    }

    public IIntegerAtom getAtom() {
        return atom;
    }

    public void setAtom(IIntegerAtom atom) {
        this.atom = atom;
    }

    HttpClient httpClient;

    public void init() {
        HttpParams params = new BasicHttpParams();
        ConnManagerParams.setMaxTotalConnections(params, 100);
        ConnManagerParams.setMaxConnectionsPerRoute(params, new ConnPerRouteBean(20));
        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);

        SchemeRegistry schemeRegistry = new SchemeRegistry();
        schemeRegistry.register(new Scheme("http", PlainSocketFactory.getSocketFactory(), 80));

        ClientConnectionManager cm = new ThreadSafeClientConnManager(params, schemeRegistry);
        httpClient = new DefaultHttpClient(cm, params);
    }

    @Override
    public String getRelatedUrl(String fileId, String spec) {
        try {
            String url = getUrl(fileId);
            int index = url.lastIndexOf(".");
            return url.substring(0, index) + "_" + spec + url.substring(index);
        } catch (Exception e) {
            System.out.println("fileId = " + fileId + ", spec = " + spec + ", getRelatedUrl exception");
            e.printStackTrace();
        }
        return null;
    }

    public String getRelatedUrl0(String fileId, String spec) {
        String[] parts = fileId.split("_");
        String globalId = parts[0];
        String localId = parts[1];
        String serverId = parts[2];
        String url = parts[3];
        PigeonFileSystemConfig.Server server = selectServer(fileId);

        int pos = url.lastIndexOf(".");
        String namePart = url.substring(0, pos);
        String extPart = url.substring(pos);

        namePart = namePart + "_" + spec;
        url = namePart + extPart;
        String urlPrefix = server.externalUrl;
        if (urlPrefix.endsWith("/")) {
            return server.externalUrl + url;
        } else {
            return server.externalUrl + "/" + url;
        }
    }

    @Override
    public void genRelatedFile(String fileId, String spec) throws Exception {
        PigeonFileSystemConfig.Server server = selectOrigServer(fileId);
        JSONObject jfileObject = CommonTools.getObject(fileId, flexObjectFactory);
        JSONObject jrelated = null;
        if (jfileObject != null) {
            jrelated = jfileObject.optJSONObject("related");
        } else {
            jfileObject = new JSONObject();
        }
        if (jrelated == null) {
            jrelated = new JSONObject();
            jfileObject.put("related", jrelated);
        }
        jrelated.put(spec, "");
        String sContent = jfileObject.toString();
        flexObjectFactory.saveContent(fileId, sContent);
        String md5 = jfileObject.optString("digest");
        if (md5.length() > 0) {
            flexObjectFactory.saveContent(md5, sContent);
        }
        try {
            executeCommand(server.writeUrl, "genRelated", new String[]{fileId, spec});
        } catch (Exception e) {
            flexObjectFactory.deleteContent(md5);
            throw e;
        }
    }


    void executeCommand(String url, String command, String[] params) throws Exception {
        HttpPost httpPost = new HttpPost(url);
        /*
        List<NameValuePair> nvps = new ArrayList<NameValuePair>();
        nvps.add(new BasicNameValuePair("action", "saveContent"));
        nvps.add(new BasicNameValuePair("name", name));
        nvps.add(new BasicNameValuePair("content", content));
        httpPost.setEntity(new UrlEncodedFormEntity(nvps, HTTP.UTF_8));
        */


        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, command);
        for (String param : params) {
            CommonTools.writeString(out, param);
        }
        httpPost.setEntity(new ByteArrayEntity(out.toByteArray()));

        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();
        if (entity != null) {
            try {
                InputStream in = entity.getContent();
                String state = CommonTools.readString(in);
                if (state.equals("ok")) {
                    return;
                } else {
                    throw new Exception("save failed." + state);
                }
            } finally {
                entity.consumeContent();
            }
        }
    }


    public IFlexObjectFactory getFlexObjectFactory() {
        return flexObjectFactory;
    }

    public void setFlexObjectFactory(IFlexObjectFactory flexObjectFactory) {
        this.flexObjectFactory = flexObjectFactory;
    }

    public PigeonFileSystemConfig getConfig() {
        return config;
    }

    public void setConfig(PigeonFileSystemConfig config) {
        this.config = config;
    }

    @Override
    public void delete(String fileid) throws Exception {
        JSONObject jfile = CommonTools.getObject(fileid, flexObjectFactory);
        if (jfile == null) return;
        String digest = jfile.optString("digest");
        long refCount = atom.greaterAndIncReturnLong(digest, 0, -1);
        if (refCount == 0) {
            flexObjectFactory.saveContent(fileid, "");
            flexObjectFactory.saveContent(digest, "");
            PigeonFileSystemConfig.Server server = selectServer(fileid);
            executeCommand(server.writeUrl, "delete", new String[]{jfile.toString()});
        }


    }

    PigeonFileSystemConfig.Server selectServer(String fileId) {
        String[] ts = fileId.split("@");
        String[] parts = ts[0].split("_");
        String id;
        if (ts.length == 2 && parts.length == 2) {
            id = parts[0];
        } else if (parts.length >= 3) {
            String globalId = parts[0];
            id = globalId;
        } else {
            // "global1_localgroup1_server1"
            id = "global1";
        }
        PigeonFileSystemConfig.Server ps = config.selectServer(id);
        if (ps == null) {
            ps = config.selectServer("lg1");
            if (ps == null) {
                ps = config.selectServer("g1");
            }
        }
        return ps;
    }

    PigeonFileSystemConfig.Server selectOrigServer(String fileId) {
        String[] ts = fileId.split("@");
        String[] parts = ts[0].split("_");
        String id;
        if (ts.length == 2 && parts.length == 2) {
            PigeonFileSystemConfig.Server svr = config.selectServer(parts[0]);
            if (svr == null) {
                svr = config.selectOrigServer("g1", parts[0], parts[1]);
            }
            return svr;
        } else if (parts.length >= 3) {
            String globalId = parts[0];
            String localId = parts[1];
            String serverId = parts[2];
            return config.selectOrigServer(globalId, localId, serverId);
        } else {
            // "global1_localgroup1_server1"
            id = "global1";
        }
        PigeonFileSystemConfig.Server svr = config.selectServer(id);
        if (svr == null) {
            svr = config.selectServer("g1");
        }
        return svr;
    }

    public String getUrl(String urlPrefix, String fileid) throws Exception {
        try {
            String[] ts = fileid.split("@");
            String[] parts = ts[0].split("_");
            if (ts.length == 1 && parts.length != 4) {
                return urlPrefix + fileid.substring(fileid.indexOf("@") + 1, fileid.length());
            }
            String url = "";
            if (ts.length > 1) {
                url = ts[1];
            } else {
                url = parts[3];
                if (parts.length == 5) {
                    url += "_" + parts[4];
                }
            }
            String m = "";
            if (!urlPrefix.endsWith("/") && !url.startsWith("/")) {
                m = "/";
            }
            return urlPrefix + m + url;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("urlPrefix = " + urlPrefix + ", fileid = " + fileid);
            throw e;
        }
    }

    @Override
    public String getUrl(String fileid) throws Exception {
        String url;
        String ofid = fileid;
        String[] ts = fileid.split("@");
        if (ts.length == 1) {
            if (fileid.split("_").length != 4 && fileid.indexOf('@') == -1) {
                String newId = flexObjectFactory.getConstant("filesystem_" + fileid);
                if (newId != null && newId.length() > 0) {
                    JSONObject jo = new JSONObject(newId);
                    fileid = jo.optString("fileId");
                }
            }
        }
        PigeonFileSystemConfig.Server server = selectServer(fileid);
        if (server == null) {
            System.out.println("PigeonFileSystem selectServer() == null, ofid = " + ofid + ", fileid = " + fileid);
        }
        String urlPrefix = server.externalUrl;
        if (server.externalUrls != null) {
            int hash = HashAlgorithms.FNVHash1(fileid);
            if (hash < 0) {
                hash = -hash;
            }
            urlPrefix = server.externalUrls[hash % server.externalUrls.length];
        }
        if (!urlPrefix.endsWith("/")) {
            // urlPrefix += "/";
        }
        url = getUrl(urlPrefix, fileid);
        return url;
    }

    @Override
    public String getInternalUrl(String fileid) throws Exception {
        String url;
        String[] ts = fileid.split("@");
        if (ts.length == 1) {
            if (fileid.split("_").length != 4 && fileid.indexOf('@') == -1) {
                String newId = flexObjectFactory.getConstant("filesystem_" + fileid);
                if (newId != null && newId.length() > 0) {
                    JSONObject jo = new JSONObject(newId);
                    fileid = jo.optString("fileId");
                }
            }
        }
        PigeonFileSystemConfig.Server server = selectServer(fileid);
        String urlPrefix = server.internalUrl;
        if (!urlPrefix.endsWith("/")) {
            urlPrefix += "/";
        }
        url = getUrl(urlPrefix, fileid);
        return url;
    }

    @Deprecated
    /**
     * 由于这个方法无法支持去重，已经废弃
     */
    public OutputStream openOutputSystem(String fileId) throws Exception {
        // throw new Exception("This method is nolonger supported");
        return new RemoteOutputStream(this, fileId);
    }

    @Override
    public String checkExists(File f) throws Exception, IOException {
        String digest = org.apache.commons.codec.digest.DigestUtils.md5Hex(new FileInputStream(f));
        JSONObject jobj = CommonTools.getObject(digest, this.flexObjectFactory);
        if (jobj == null) {
            return null;
        } else {
            return jobj.getString("fileId");
        }

    }


    //GlobalGroup
    //selectGlobalGroup
    //按一定的逻辑选择GlobalGorup
    //目前主要会按照每个GlobalGroup的文件数来选择
    static int count = 0;

    PigeonFileSystemConfig.GlobalGroup selectGlobalGroupForUpload() {
        int max = 0;
        List<PigeonFileSystemConfig.GlobalGroup> groups = new Vector<PigeonFileSystemConfig.GlobalGroup>();
        for (PigeonFileSystemConfig.GlobalGroup g : config.globalGroups) {
            if (g.priority > max) {
                groups.clear();
                groups.add(g);
                max = g.priority;
            } else if (g.priority == max) {
                groups.add(g);
            }
        }
        count++;
        return groups.get(count % groups.size());
    }

    String getFilePath() throws Exception {
        /*SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");
        String today = format.format(new Date());
        String fileId = "/" + today;*/
        Calendar calendar = Calendar.getInstance();
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH) + 1;
        int day = calendar.get(Calendar.DAY_OF_MONTH);
        String fileId = "/" + year + "/" + month + "/" + day;
        String id = fileId + "/" + idGenerator.getId("files");
        return id;
    }

    void uploadToServer(String fileId, PigeonFileSystemConfig.Server server, File f) throws Exception {
        HttpEntity entity = null;
        FileInputStream fin = null;
        try {
            if (!f.exists()) {
                logger.warning("fail to upload file, file " + f.getAbsolutePath() + " does not exists.");
                return;
            }
            long fileLen = f.length();
            HttpPost httpPost = new HttpPost(server.writeUrl);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            CommonTools.writeString(out, "upload");
            CommonTools.writeString(out, fileId);
            CommonTools.writeLong(out, fileLen);

            fin = new FileInputStream(f);


            int n = 0;
            byte[] buf = new byte[2048];
            while ((n = fin.read(buf)) != -1) {
                CommonTools.writeBytes(out, buf, 0, n);
            }
            httpPost.setEntity(new ByteArrayEntity(out.toByteArray()));
            HttpResponse response = httpClient.execute(httpPost);
            entity = response.getEntity();
            if (entity != null) {
                InputStream in = entity.getContent();
                String state = CommonTools.readString(in);
                if (state.equals("ok")) {
                    return;
                } else {
                    logger.log(Level.SEVERE, "upload file error,server return wrong msg: " + state);
                }
            }
        } finally {
            if (fin != null) {
                fin.close();
            }
            if (entity != null) {
                entity.consumeContent();
            }
        }
    }

    void uploadToServer(String fileId, PigeonFileSystemConfig.Server server, byte[] bytes) throws Exception {
        HttpEntity entity = null;
        try {
            System.out.println("filesystem post url = " + server.writeUrl + ", fileId = " + fileId);
            int fileLen = bytes.length;
            HttpPost httpPost = new HttpPost(server.writeUrl);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            CommonTools.writeString(out, "upload");
            CommonTools.writeString(out, fileId);
            CommonTools.writeLong(out, fileLen);
            CommonTools.writeBytes(out, bytes, 0, fileLen);

            httpPost.setEntity(new ByteArrayEntity(out.toByteArray()));
            HttpResponse response = httpClient.execute(httpPost);
            entity = response.getEntity();
            if (entity != null) {
                InputStream in = entity.getContent();
                String state = CommonTools.readString(in);
                if (state.equals("ok")) {
                    return;
                } else {
                    logger.log(Level.SEVERE, "upload file error,server return wrong msg: " + state);
                }
            }
        } finally {
            if (entity != null) {
                entity.consumeContent();
            }
        }
    }

    String getExtension(File f) {
        String fullPath = f.getName();
        int rpos = fullPath.lastIndexOf(".");
        if (rpos < 0) {
            return "";
        } else {
            return fullPath.substring(rpos);
        }

    }

    String getExtension(String fullPath) {
        int rpos = fullPath.lastIndexOf(".");
        if (rpos < 0) {
            return "";
        } else {
            return fullPath.substring(rpos);
        }
    }


    @Override
    public String addFile(File f, String name) throws Exception {
        if (!f.exists()) {
            logger.info(f.getAbsolutePath() + " does not existed.");
            return null;
        }
        String digest = org.apache.commons.codec.digest.DigestUtils.md5Hex(new FileInputStream(f));
        JSONObject jobj = CommonTools.getObject(digest, this.flexObjectFactory);
        if (jobj == null) {
            PigeonFileSystemConfig.GlobalGroup globalGroup;
            globalGroup = selectGlobalGroupForUpload();

            PigeonFileSystemConfig.LocalGroup lg = globalGroup.preferedLocalGroup;

            PigeonFileSystemConfig.Server server = config.selectServerFromLocalGroup(lg);

            String gid = globalGroup.id;
            String lid = lg.id;
            String serverId = server.serverId;

            String fileId = gid + "_" + lid + "_" + serverId + "@" + getFilePath() + getExtension(f);


            //上传到Server
            uploadToServer(fileId, server, f);

            //保存到Pigeon
            JSONObject jfile = new JSONObject();
            jfile.put("id", digest);
            jfile.put("fileId", fileId);
            atom.createAndSet(digest, 1);
            jfile.put("size", f.length());
            flexObjectFactory.saveContent(digest, jfile.toString());

            jfile.put("id", fileId);
            jfile.put("digest", digest);
            flexObjectFactory.saveContent(fileId, jfile.toString());
            //保存到队列
            List<PigeonFileSystemConfig.Server> servers = config.getServers(globalGroup);
            for (PigeonFileSystemConfig.Server s : servers) {
                if (serverId.equals(s.serverId)) {
                    continue;
                }
                ISortList list = listFactory.getList(s.serverId + "_downloadlist", true);
                SortListObject sobj = new SortListObject();
                sobj.setKey(CommonTools.getComparableString(System.currentTimeMillis(), 12));
                sobj.setObjid(fileId);
                list.add(sobj);
                System.out.println(s.serverId + "_downloadlist += " + fileId);
            }
            return fileId;
        } else {
            String fileId = jobj.getString("fileId");
            atom.greaterAndInc(digest, 0, 1);
            return jobj.getString("fileId");
        }
    }

    @Override
    public String addBytes(byte[] bytes, String name) throws Exception {
        String digest = org.apache.commons.codec.digest.DigestUtils.md5Hex(bytes);
        JSONObject jobj = CommonTools.getObject(digest, this.flexObjectFactory);
        if (jobj != null) {
            String fileId = jobj.getString("fileId");
            /*String oldId = "filesystem_" + name;
            jobj.put("id", oldId);
            flexObjectFactory.saveContent(oldId, jobj.toString());*/
            try {
                atom.greaterAndInc(digest, 0, 1);
            }
            catch(Exception e){
                atom.createAndSet(digest, 1);
            }
            return fileId;
        }

        PigeonFileSystemConfig.GlobalGroup globalGroup;
        globalGroup = selectGlobalGroupForUpload();

        PigeonFileSystemConfig.LocalGroup lg = globalGroup.preferedLocalGroup;

        PigeonFileSystemConfig.Server server = config.selectServerFromLocalGroup(lg);

        String gid = globalGroup.id;
        String lid = lg.id;
        String serverId = server.serverId;

        String fileId = gid + "_" + lid + "_" + serverId + "@" + getFilePath() + getExtension(name);

        //上传到Server
        uploadToServer(fileId, server, bytes);

        //保存到Pigeon
        JSONObject jfile = new JSONObject();
        jfile.put("fileId", fileId);
        jfile.put("oldId", name);
        jfile.put("size", bytes.length);
        jfile.put("digest", digest);
        jfile.put("id", digest);
        atom.createAndSet(digest, 1);
        flexObjectFactory.saveContent(digest, jfile.toString());
        jfile.put("id", fileId);
        flexObjectFactory.saveContent(fileId, jfile.toString());
        /*String oldId = "filesystem_" + name;
        jfile.put("id", oldId);
        flexObjectFactory.saveContent(oldId, jfile.toString());*/
        //保存到队列
        List<PigeonFileSystemConfig.Server> servers = config.getServers(globalGroup);
        for (PigeonFileSystemConfig.Server s : servers) {
            if (serverId.equals(s.serverId)) {
                continue;
            }
            ISortList list = listFactory.getList(s.serverId + "_downloadlist", true);
            SortListObject sobj = new SortListObject();
            sobj.setKey(CommonTools.getComparableString(System.currentTimeMillis(), 12));
            sobj.setObjid(fileId);
            list.add(sobj);
            System.out.println(s.serverId + "_downloadlist += " + fileId);
        }
        return fileId;
    }

}
