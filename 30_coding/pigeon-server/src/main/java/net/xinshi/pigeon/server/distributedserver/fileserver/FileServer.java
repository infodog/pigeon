package net.xinshi.pigeon.server.distributedserver.fileserver;

import com.sun.imageio.plugins.jpeg.JPEGImageWriter;
import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.distributed.bean.ServerConfig;
import net.xinshi.pigeon.filesystem.impl.PigeonFileSystemConfig;
import net.xinshi.pigeon.list.ISortList;
import net.xinshi.pigeon.list.SortListObject;
import net.xinshi.pigeon.server.distributedserver.BaseServer;
import net.xinshi.pigeon.util.CommonTools;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.params.ConnManagerParams;
import org.apache.http.conn.params.ConnPerRouteBean;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.imgscalr.Scalr;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.plugins.jpeg.JPEGImageWriteParam;
import javax.imageio.stream.ImageOutputStream;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午4:30
 * To change this template use File | Settings | File Templates.
 */

public class FileServer extends BaseServer {

    ServerConfig sc;
    String baseDir;
    IPigeonStoreEngine pigeonStoreEngine;
    Logger logger = Logger.getLogger(FileServer.class.getName());

    HttpClient downloadHttpClient;
    boolean stopping;
    boolean stopped;
    Object waiter;
    Object stoppedMonitor;
    Thread downloaderThread;

    public FileServer(ServerConfig sc) {
        this.sc = sc;
        setBaseDir(sc.getBaseDir());
        stopping = false;
        stopped = false;
        waiter = new Object();
        stoppedMonitor = new Object();
    }

    public void stop() {
        synchronized (waiter) {
            stopping = true;
            try {
                pigeonStoreEngine.stop();
            } catch (InterruptedException e) {
                logger.warning("pigeonStoreEngine already stopped where stop file server.");
            }
            waiter.notify();
        }
        synchronized (stoppedMonitor) {
            while (!stopped) {
                try {
                    stoppedMonitor.wait(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    String meID = null;
    PigeonFileSystemConfig pigeonFileSystemConfig;

    public PigeonFileSystemConfig getPigeonFileSystemConfig() {
        return pigeonFileSystemConfig;
    }

    public void setPigeonFileSystemConfig(PigeonFileSystemConfig pigeonFileSystemConfig) {
        this.pigeonFileSystemConfig = pigeonFileSystemConfig;
        meID = pigeonFileSystemConfig.getServerFullID(this.sc.getName());
        System.out.println("my full id = " + meID);
    }

    void init() {
        if(getPigeonFileSystemConfig().getServers(sc.getName()).size()>1){
            HttpParams params = new BasicHttpParams();
            ConnManagerParams.setMaxTotalConnections(params, 100);
            ConnManagerParams.setMaxConnectionsPerRoute(params, new ConnPerRouteBean(30));
            HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
            SchemeRegistry schemeRegistry = new SchemeRegistry();
            schemeRegistry.register(new Scheme("http", PlainSocketFactory.getSocketFactory(), 80));
            ClientConnectionManager cm = new ThreadSafeClientConnManager(params, schemeRegistry);
            downloadHttpClient = new DefaultHttpClient(cm, params);
            downloaderThread = new Thread(new Downloader());
            downloaderThread.start();
        }
        else{
            logger.info("file server " + sc.getName() + " no need to start download thread.");
        }

    }

    public IPigeonStoreEngine getPigeonStoreEngine() {
        return pigeonStoreEngine;
    }

    public void setPigeonStoreEngine(IPigeonStoreEngine pigeonStoreEngine) {
        this.pigeonStoreEngine = pigeonStoreEngine;
    }

    public void setBaseDir(String baseDir) {
        this.baseDir = baseDir;
        File f = new File(baseDir);
        f.mkdirs();
    }

    String getFilePathFromFileId(String fileId) {
        String[] ts = fileId.split("@");
        String[] parts = ts[0].split("_");
        String url = "";
        if (ts.length > 1) {
            url = ts[1];
        } else {
            url = parts[3];
            if (parts.length == 5) {
                url += "_" + parts[4];
            }
        }
        if (baseDir.endsWith("/")) {
            return baseDir + url;
        } else {
            return baseDir + "/" + url;
        }
    }

    String getPath(String filePath) {
        int pos = filePath.lastIndexOf("/");
        if (pos < 0) {
            return "";
        }
        return filePath.substring(0, pos);
    }

    public void doGenRelated(InputStream is, OutputStream os) throws Exception {
        long begin = System.currentTimeMillis();
        try {
            if (!pigeonFileSystemConfig.getGlobalGroup(sc.getName()).genRelated) {
                CommonTools.writeString(os, "ok");
                return;
            }
            String fileId = CommonTools.readString(is);
            String spec = CommonTools.readString(is);
            String filePath = getFilePathFromFileId(fileId);
            File origFile = new File(filePath);
            filePath = origFile.getAbsolutePath();
            int pos = filePath.lastIndexOf(".");
            String namePart = filePath.substring(0, pos);
            String extPart = filePath.substring(pos + 1);
            namePart = namePart + "_" + spec;
            String relatedfilePath = namePart + "." + extPart;
            File f = new File(getPath(relatedfilePath));
            f = new File(f.getAbsolutePath());
            f.mkdirs();
            f = new File(relatedfilePath);
            if (!f.exists() || f.length() == 0) {
                // System.out.println("doGenRelated ... " + f.getAbsolutePath() + " exists, skip " + spec);
                // CommonTools.writeString(os, "ok");
                // return;
                if (StringUtils.equalsIgnoreCase(extPart, "jpg") || StringUtils.equalsIgnoreCase(extPart, "jpeg") || StringUtils.equalsIgnoreCase(extPart, "gif") || StringUtils.equalsIgnoreCase(extPart, "png")) {
                    FileInputStream fis = new FileInputStream(filePath);
                    BufferedImage img = ImageIO.read(fis);
                    fis.close();
                    String[] xy = spec.split("X");
                    int x = Integer.parseInt(xy[0]);
                    int y = Integer.parseInt(xy[1]);
                    BufferedImage resizedImage = Scalr.resize(img, x, y, null);
                    ByteArrayOutputStream bs = new ByteArrayOutputStream();
                    if (extPart.equalsIgnoreCase("jpg") || extPart.equalsIgnoreCase("jpeg")) {
                        try {
                            JPEGImageWriter imageWriter = (JPEGImageWriter) ImageIO.getImageWritersBySuffix("jpeg").next();
                            ImageOutputStream ios = ImageIO.createImageOutputStream(bs);
                            imageWriter.setOutput(ios);
                            JPEGImageWriteParam jpegParams = (JPEGImageWriteParam) imageWriter.getDefaultWriteParam();
                            jpegParams.setCompressionMode(JPEGImageWriteParam.MODE_EXPLICIT);
                            jpegParams.setCompressionQuality(0.95f);
                            IIOMetadata data = imageWriter.getDefaultImageMetadata(new ImageTypeSpecifier(resizedImage), jpegParams);
                            imageWriter.write(data, new IIOImage(resizedImage, null, null), jpegParams);
                            bs.close();
                            imageWriter.dispose();
                        } catch (Exception e) {
                            e.printStackTrace();
                            return;
                        }
                    } else {
                        ImageOutputStream imOut = ImageIO.createImageOutputStream(bs);
                        ImageIO.write(resizedImage, extPart, imOut);
                    }
                    logger.info("saving file " + f.getAbsolutePath());
                    FileOutputStream fos = new FileOutputStream(f.getAbsolutePath());
                    fos.write(bs.toByteArray());
                    fos.close();
                } else {
                    CommonTools.writeString(os, "not recognized image type");
                    return;
                }
            }
            {
                //保存到队列
                String serverId = sc.getName();
                List<PigeonFileSystemConfig.Server> servers = pigeonFileSystemConfig.getServers(serverId);
                for (PigeonFileSystemConfig.Server s : servers) {
                    if (serverId.equals(s.serverId)) {
                        continue;
                    }
                    ISortList list = pigeonStoreEngine.getListFactory().getList(s.serverId + "_downloadlist", true);
                    SortListObject sobj = new SortListObject();
                    sobj.setKey(CommonTools.getComparableString(System.currentTimeMillis(), 12));
                    int index = fileId.lastIndexOf(".");
                    String sfileId = fileId.substring(0, index) + "_" + spec + fileId.substring(index);
                    index = sfileId.indexOf('@');
                    sfileId = meID + sfileId.substring(index);
                    sobj.setObjid(sfileId);
                    list.add(sobj);
                    System.out.println(s.serverId + "_downloadlist += " + sfileId);
                }
            }
            CommonTools.writeString(os, "ok");
            long end = System.currentTimeMillis();
            logger.info("gen file consumed time:" + (end - begin) + "ms");
            return;
        } catch (Exception e) {
            e.printStackTrace();
            logger.warning("error creating related images, err msg is :" + e.getMessage());
            CommonTools.writeString(os, "error creating related images, err msg is :" + e.getMessage());
        }
    }

    public void doSaveFile(InputStream is, OutputStream os) throws Exception {
        String fileId = CommonTools.readString(is);
        long fileLen = CommonTools.readLong(is);
        String filePath = getFilePathFromFileId(fileId);
        if (filePath == null) {
            throw new Exception("no file path");
        }
        File f = new File(getPath(filePath));
        f = new File(f.getAbsolutePath());
        f.mkdirs();
        f = new File(filePath);
        System.out.println("saving file : " + f.getAbsolutePath());
        FileOutputStream fos = new FileOutputStream(f.getAbsolutePath());
        while (fileLen > 0) {
            byte[] bytes = CommonTools.readBytes(is);
            if (bytes == null) {
                throw new Exception("file upload broken");
            }
            fileLen -= bytes.length;
            fos.write(bytes);
        }
        fos.close();
        InfoZipFile.decompressFile(f.getAbsolutePath());
        try {
            String serverId = sc.getName();
            List<PigeonFileSystemConfig.Server> servers = pigeonFileSystemConfig.getServers(serverId);
            for (PigeonFileSystemConfig.Server s : servers) {
                if (serverId.equals(s.serverId)) {
                    continue;
                }
                ISortList list = pigeonStoreEngine.getListFactory().getList(s.serverId + "_downloadlist", true);
                SortListObject sobj = new SortListObject();
                sobj.setKey(CommonTools.getComparableString(System.currentTimeMillis(), 12));
                sobj.setObjid(fileId);
                list.add(sobj);
                System.out.println(s.serverId + "_downloadlist += " + fileId);
            }
        } catch (Exception e) {
            System.out.println("notify file download list failed! please check pigeonfilesystem.conf. fileId = " + fileId);
        }
        CommonTools.writeString(os, "ok");
    }

    public void doDelete(InputStream is, OutputStream os) throws Exception {
        try {
            String sFileObject = CommonTools.readString(is);
            JSONObject jfile = new JSONObject(sFileObject);
            String fileId = jfile.getString("fileId");
            String filePath = getFilePathFromFileId(fileId);
            if (filePath == null) {
                throw new Exception("no file path");
            }
            File f = new File(filePath);
            f = new File(f.getAbsolutePath());
            logger.warning("deleting file :" + f.getAbsolutePath());
            f.delete();
            String origFilePath = f.getAbsolutePath();
            int pos = origFilePath.lastIndexOf(".");
            if (pos > 0) {
                String namePart = origFilePath.substring(0, pos);
                String extPart = origFilePath.substring(pos + 1);
                JSONArray jrelated = jfile.optJSONArray("related");
                if (jrelated != null) {
                    for (int i = 0; i < jrelated.length(); i++) {
                        String spec = jrelated.getString(i);
                        if (spec != null) {
                            File relatedFile = new File(namePart + "_" + spec + "." + extPart);
                            if (relatedFile.exists()) {
                                relatedFile.delete();
                            }
                        }
                    }
                }
            }
            CommonTools.writeString(os, "ok");
        } catch (Exception e) {
            CommonTools.writeString(os, "error:" + e.getMessage());
        }
    }

    String getFullPathFromFileId(String fileId) {
        String[] ts = fileId.split("@");
        String[] parts = ts[0].split("_");
        String url = "";
        if (ts.length > 1) {
            url = ts[1];
        } else {
            url = parts[3];
            if (parts.length == 5) {
                url += "_" + parts[4];
            }
        }
        if (baseDir.endsWith("/")) {
            return baseDir + url;
        } else {
            return baseDir + "/" + url;
        }
    }

    void makeDirs(String dir) {
        String t = dir.replace("\\", "/");
        int last = t.lastIndexOf("/");
        t = t.substring(0, last);
        File f = new File(t);
        if (!f.exists()) {
            f.mkdirs();
        }
    }

    String getInternalUrl(String fileId) {
        String[] ts = fileId.split("@");
        String[] parts = ts[0].split("_");
        String serverId = parts[1];
        if (parts.length > 2) {
            serverId = parts[2];
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
        if (serverId.equals(sc.getName())) {
            return null;
        }
        PigeonFileSystemConfig.Server server = pigeonFileSystemConfig.getServer(serverId);
        String m = "/";
        if (server.internalUrl.endsWith("/") || url.startsWith("/")) {
            m = "";
        }
        return server.internalUrl + m + url;
    }

    class Downloader implements Runnable {
        void downloadFile(String fileId) throws Exception {
            String downloadUrl = getInternalUrl(fileId);
            if (downloadUrl == null) {
                System.out.println("bad downloadUrl : " + fileId + ", ignore ......");
                return;
            }
            HttpGet get = new HttpGet(downloadUrl);
            System.out.println("downloading " + downloadUrl);
            HttpEntity entity = null;
            try {
                HttpResponse response = downloadHttpClient.execute(get);
                if (response.getStatusLine().getStatusCode() != 200) {
                    throw new Exception("response.getStatusLine().getStatusCode() != 200");
                }
                entity = response.getEntity();
                String filepath = getFullPathFromFileId(fileId);
                File fe = new File(filepath);
                if (fe.exists() && fe.length() > 0) {
                    System.out.println("file exists : " + fileId + ", ignore ......" + filepath);
                    return;
                }
                makeDirs(filepath);
                FileOutputStream fos = new FileOutputStream(filepath);
                InputStream in = entity.getContent();
                byte[] buf = new byte[2048];
                int n = in.read(buf);
                while (n > 0) {
                    fos.write(buf, 0, n);
                    n = in.read(buf);
                }
                fos.close();
                InfoZipFile.decompressFile(filepath);
            } finally {
                if (entity != null) {
                    entity.consumeContent();
                }
            }
        }

        @Override
        public void run() {
            Thread.currentThread().setName("FileServer_run");
            try {
                Thread.sleep(1000 * 60);
            } catch (Exception e) {
                e.printStackTrace();
            }
            while (!stopping) {
                try {
                    long sleep = 1000;
                    ISortList que = pigeonStoreEngine.getListFactory().getList(sc.getName() + "_downloadlist", true);
                    SortListObject bad = null;
                    try {
                        while (que.getSize() > 0) {
                            List<SortListObject> files = que.getRange(0, 10);
                            for (SortListObject sobj : files) {
                                System.out.println("filesystem download : " + sobj.getObjid());
                                bad = sobj;
                                downloadFile(sobj.getObjid());
                                que.delete(sobj);
                                bad = null;
                            }
                        }
                    } catch (Exception e) {
                        if (bad != null) {
                            sleep *= 10;
                            System.out.println("filesystem download : " + bad.getObjid() + " failed ... " + e.getMessage());
                            e.printStackTrace();
                            que.delete(bad);
                            bad.setKey(CommonTools.getComparableString(System.currentTimeMillis(), 12));
                            que.add(bad);
                        }
                    }
                    synchronized (waiter) {
                        waiter.wait(sleep);
                        if (stopping) {
                            break;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    try {
                        synchronized (waiter) {
                            waiter.wait(1000);
                            if (stopping) {
                                break;
                            }
                        }
                    } catch (InterruptedException e1) {
                        logger.log(Level.SEVERE, "FileServer_run error : ", e);
                    }
                }
            }
            synchronized (stoppedMonitor) {
                stopped = true;
                stoppedMonitor.notify();
            }
        }
    }
}

