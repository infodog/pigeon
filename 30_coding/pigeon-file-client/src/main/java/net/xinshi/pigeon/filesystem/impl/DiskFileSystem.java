package net.xinshi.pigeon.filesystem.impl;

import net.xinshi.pigeon.filesystem.IFileSystem;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by IntelliJ IDEA.
 * User: zxy
 * Date: 2010-7-2
 * Time: 10:27:04
 * To change this template use File | Settings | File Templates.
 */

public class DiskFileSystem implements IFileSystem {
    String rootPath;
    String urlPrefix;
    String internalUrlPrefix;

    public String getInternalUrlPrefix() {
        return internalUrlPrefix;
    }

    public void setInternalUrlPrefix(String internalUrlPrefix) {
        this.internalUrlPrefix = internalUrlPrefix;
    }

    public String getRootPath() {
        return rootPath;
    }

    public void setRootPath(String rootPath) {
        this.rootPath = rootPath;
    }

    public String getDiskPath(String fileid) {
        return getRootPath() + fileid;
    }


    public String getUrlPrefix() {
        return urlPrefix;
    }

    public void setUrlPrefix(String urlPrefix) {
        this.urlPrefix = urlPrefix;
    }

    public void delete(String fileid) throws Exception {

        FileUtils.forceDelete(new File(getDiskPath(fileid)));

    }

    public String getUrl(String fileid) throws Exception {
        return urlPrefix + fileid;
    }

    public String getInternalUrl(String fileid) throws Exception {
        return internalUrlPrefix + fileid;
    }


    public OutputStream openOutputSystem(String fileid) throws Exception {
        return FileUtils.openOutputStream(new File(getDiskPath(fileid)));
    }

    @Override
    public String checkExists(File f) throws Exception, IOException {
        throw new Exception("not Implementation ...... ");
    }

    @Override
    public String addFile(File f, String name) throws Exception {
        throw new Exception("not Implementation ...... ");
    }

    @Override
    public String addBytes(byte[] bytes, String name) throws Exception {
        throw new Exception("not Implementation ...... ");
    }

    public void init() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getRelatedUrl(String fileId, String spec) {
        System.out.println("getRelatedUrl not Implementation ...... ");
        return null;
    }

    @Override
    public void genRelatedFile(String fileId, String spec) throws Exception {
        throw new Exception("not Implementation ...... ");
    }
}
