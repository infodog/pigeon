package net.xinshi.pigeon.server.standalongserver.fileserver;

import com.sun.imageio.plugins.jpeg.JPEGImageWriter;
import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.filesystem.impl.PigeonFileSystemConfig;
import net.xinshi.pigeon.list.ISortList;
import net.xinshi.pigeon.list.SortListObject;
import net.xinshi.pigeon.server.standalongserver.BaseServer;
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
 * User: mac
 * Date: 11-12-1
 * Time: 下午3:49
 * To change this template use File | Settings | File Templates.
 */
public class FileServer extends BaseServer {
    String baseDir;
    String name;
    IPigeonStoreEngine pigeonStoreEngine;
    Logger logger = Logger.getLogger("FileServer");

    HttpClient downloadHttpClient;
    boolean stopping;
    boolean stopped;
    Object waiter;
    Object stoppedMonitor;
    Thread downloaderThread;

    public FileServer() {
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
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }
    }

    PigeonFileSystemConfig pigeonFileSystemConfig;

    public PigeonFileSystemConfig getPigeonFileSystemConfig() {
        return pigeonFileSystemConfig;
    }

    public void setPigeonFileSystemConfig(PigeonFileSystemConfig pigeonFileSystemConfig) {
        this.pigeonFileSystemConfig = pigeonFileSystemConfig;
    }

    void init() {

        // Create and initialize HTTP parameters
        HttpParams params = new BasicHttpParams();
        ConnManagerParams.setMaxTotalConnections(params, 100);
        ConnManagerParams.setMaxConnectionsPerRoute(params, new ConnPerRouteBean(20));
        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);

        // Create and initialize scheme registry
        SchemeRegistry schemeRegistry = new SchemeRegistry();
        schemeRegistry.register(new Scheme("http", PlainSocketFactory.getSocketFactory(), 80));

        // Create an HttpClient with the ThreadSafeClientConnManager.
        // This connection manager must be used if more than one thread will
        // be using the HttpClient.
        ClientConnectionManager cm = new ThreadSafeClientConnManager(params, schemeRegistry);
        downloadHttpClient = new DefaultHttpClient(cm, params);

        downloaderThread = new Thread(new Downloader());
        downloaderThread.start();


    }

    public IPigeonStoreEngine getPigeonStoreEngine() {
        return pigeonStoreEngine;
    }

    public void setPigeonStoreEngine(IPigeonStoreEngine pigeonStoreEngine) {
        this.pigeonStoreEngine = pigeonStoreEngine;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getBaseDir() {
        return baseDir;
    }

    public void setBaseDir(String baseDir) {
        this.baseDir = baseDir;
        File f = new File(baseDir);
        f.mkdirs();
    }

    String getFilePathFromFileId(String fileId) {
        String[] parts = fileId.split("_");
        String globalId = parts[0];
        String localId = parts[1];
        String serverId = parts[2];
        String url = parts[3];
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
            String fileId = CommonTools.readString(is);
            String spec = CommonTools.readString(is);
            String filePath = getFilePathFromFileId(fileId);
            File origFile = new File(filePath);
            filePath = origFile.getAbsolutePath();

            int pos = filePath.lastIndexOf(".");
            String namePart = filePath.substring(0, pos);
            String extPart = filePath.substring(pos + 1);
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
                        // Compression
                        JPEGImageWriteParam jpegParams = (JPEGImageWriteParam) imageWriter.getDefaultWriteParam();
                        jpegParams.setCompressionMode(JPEGImageWriteParam.MODE_EXPLICIT);
                        jpegParams.setCompressionQuality(0.95f);
                        // Metadata (dpi)
                        IIOMetadata data = imageWriter.getDefaultImageMetadata(new ImageTypeSpecifier(resizedImage), jpegParams);

                        // Write and clean up
                        imageWriter.write(data, new IIOImage(resizedImage, null, null), jpegParams);
                        bs.close();
                        imageWriter.dispose();
                    } catch (Exception e) {
                        e.printStackTrace();
                        return;//出异常就不再继续了
                    }
                } else {
                    ImageOutputStream imOut = ImageIO.createImageOutputStream(bs);
                    ImageIO.write(resizedImage, extPart, imOut);
                }

                namePart = namePart + "_" + spec;
                String relatedfilePath = namePart + "." + extPart;

                File f = new File(getPath(relatedfilePath));
                f = new File(f.getAbsolutePath());
                f.mkdirs();
                f = new File(relatedfilePath);
                logger.info("saving file " + f.getAbsolutePath());
                FileOutputStream fos = new FileOutputStream(f.getAbsolutePath());
                fos.write(bs.toByteArray());
                fos.close();
                CommonTools.writeString(os, "ok");
                long end = System.currentTimeMillis();
                logger.info("gen file consumed time:" + (end - begin) + "ms");
                return;
            }
            CommonTools.writeString(os, "not recognized image type");
        } catch (Exception e) {
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

        logger.info("saving file " + f.getAbsolutePath());
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
            //删除related

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
        String[] parts = fileId.split("_");
        String globalId = parts[0];
        String localId = parts[1];
        String serverId = parts[2];
        String url = parts[3];
        return baseDir + url;
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

    boolean isMyself(String fileId) {
        String[] parts = fileId.split("_");
        String serverId = parts[2];
        return serverId.equals(name);
    }

    String getInternalUrl(String fileId) {
        String[] parts = fileId.split("_");
        String globalId = parts[0];
        String localId = parts[1];
        String serverId = parts[2];
        String url = parts[3];
        PigeonFileSystemConfig.Server server = pigeonFileSystemConfig.getServer(serverId);
        return server.internalUrl + url;
    }

    class Downloader implements Runnable {

        void downloadFile(String fileId) throws Exception {
            HttpGet get = new HttpGet(getInternalUrl(fileId));
            HttpEntity entity = null;
            try {
                downloadHttpClient.execute(get);
                HttpResponse response = httpClient.execute(get);
                entity = response.getEntity();
                String filepath = getFullPathFromFileId(fileId);
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
                Thread.sleep(1000 * 30);
            } catch (Exception e) {
                e.printStackTrace();
            }
            while (!stopping) {
                try {
                    ISortList que = pigeonStoreEngine.getListFactory().getList(name + "_downloadlist", true);

                    while (que.getSize() > 0) {
                        List<SortListObject> files = que.getRange(0, 10);
                        for (SortListObject sobj : files) {
                            System.out.println("filesystem download : " + sobj.getObjid());
                            downloadFile(sobj.getObjid());
                            que.delete(sobj);
                        }
                    }
                    synchronized (waiter) {
                        waiter.wait(1000);
                        if (stopping) {
                            break;
                        }
                    }

                } catch (Exception e) {
                    logger.log(Level.SEVERE, "can not get downlodlist");
                    try {
                        synchronized (waiter) {
                            waiter.wait(1000);
                            if (stopping) {
                                break;
                            }
                        }
                    } catch (InterruptedException e1) {
                        logger.log(Level.SEVERE, "can not get downlodlist", e);
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
