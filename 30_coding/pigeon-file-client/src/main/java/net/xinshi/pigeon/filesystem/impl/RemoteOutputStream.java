package net.xinshi.pigeon.filesystem.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-6
 * Time: 上午9:24
 * To change this template use File | Settings | File Templates.
 */

public class RemoteOutputStream extends OutputStream {

    private ByteArrayOutputStream os = new ByteArrayOutputStream();
    private PigeonFileSystem pfs;
    private String fileId;

    public RemoteOutputStream(PigeonFileSystem pfs, String fileId) {
        this.pfs = pfs;
        this.fileId = fileId;
    }

    @Override
    public void write(int b) throws IOException {
        os.write(b);
    }

    public void flush() {
        try {
            String newID = pfs.addBytes(os.toByteArray(), fileId);
            System.out.println("oldId = " + fileId + ", newId = " + newID);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}

