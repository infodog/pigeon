package net.xinshi.pigeon.server.standalongserver.flexobjectServer;

import net.xinshi.pigeon.flexobject.FlexObjectEntry;
import net.xinshi.pigeon.flexobject.IFlexObjectFactory;
import net.xinshi.pigeon.flexobject.impls.fastsimple.SimpleFlexObjectFactory;
import net.xinshi.pigeon.server.standalongserver.BaseServer;
import net.xinshi.pigeon.util.CommonTools;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: zhengxiangyang
 * Date: 11-10-30
 * Time: 上午1:20
 * To change this template use File | Settings | File Templates.
 */
public class FlexObjectServer extends BaseServer {
    SimpleFlexObjectFactory flexObjectFactory;

    public FlexObjectServer() {
        super();
    }

    Logger logger = Logger.getLogger("FlexObjectServer");

    public IFlexObjectFactory getFlexObjectFactory() {
        return flexObjectFactory;
    }

    public void setFlexObjectFactory(SimpleFlexObjectFactory flexObjectFactory) {
        this.flexObjectFactory = flexObjectFactory;
    }

    public void doGetContent(InputStream is, OutputStream os) throws Exception {
        String name = CommonTools.readString(is);
        try {
            String content = flexObjectFactory.getContent(name);
            if (content == null) {
                content = "";
            }
            CommonTools.writeString(os, "OK");
            CommonTools.writeString(os, content);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void doGetContents(InputStream is, OutputStream os) throws Exception {

        List<String> names = new Vector();
        while (true) {
            try {
                String name = CommonTools.readString(is);
                if (name == null) {
                    break;
                }
                names.add(name);
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }
        List lnames = new ArrayList<String>();
        for (String name : names) {
            lnames.add(name);
        }
        List<String> contents = this.flexObjectFactory.getContents(lnames);
        CommonTools.writeString(os, "OK");
        for (String content : contents) {
            CommonTools.writeString(os, content);
        }
    }

    public void doSaveContent(InputStream is, OutputStream os) throws Exception {
        String name = CommonTools.readString(is);
        String content = CommonTools.readString(is);
        try {
            ++this.writes;
            flexObjectFactory.saveContent(name, content);
            CommonTools.writeString(os, "ok");
        } catch (Exception e) {
            CommonTools.writeString(os, e.getMessage());
        }
    }

    public void doSaveFlexObject(InputStream is, OutputStream os) throws Exception {
        try {
            FlexObjectEntry entry = CommonTools.readEntry(is);
            if (entry == null) {
                CommonTools.writeString(os, "incorrect request to server.");
                logger.log(Level.SEVERE, "incorrect request to server.");
            } else {
                writes++;
                flexObjectFactory.saveFlexObject(entry);
                CommonTools.writeString(os, "ok");
            }
        } catch (Exception e) {
            CommonTools.writeString(os, e.getMessage());
            logger.log(Level.SEVERE, e.getMessage(), e);
        }

    }

    public void doSaveFlexObjects(InputStream is, OutputStream os) throws IOException {
        List<FlexObjectEntry> objs = new Vector<FlexObjectEntry>();

        try {
            FlexObjectEntry entry = CommonTools.readEntry(is);
            while (entry != null) {
                objs.add(entry);
                entry = CommonTools.readEntry(is);
            }
            flexObjectFactory.saveFlexObjects(objs);
            if (flexObjectFactory.getDirtyCacheSize() > 60000) {
                CommonTools.writeString(os, "TooFast");
                CommonTools.writeLong(os, flexObjectFactory.getDirtyCacheSize());
            } else {
                CommonTools.writeString(os, "ok");
            }
        } catch (Exception e) {
            CommonTools.writeString(os, e.getMessage());
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    public void doGetFlexObject(InputStream is, OutputStream os) throws Exception {
        try {
            String name = CommonTools.readString(is);
            FlexObjectEntry entry = flexObjectFactory.getFlexObject(name);
            if (entry == null) {
                CommonTools.writeString(os, "isnull");
            } else {
                CommonTools.writeString(os, "ok");
                CommonTools.writeEntry(os, entry);
            }
        } catch (Exception e) {
            CommonTools.writeString(os, e.getMessage());
            logger.log(Level.SEVERE, e.getMessage(), e);
        }

    }

    public void doGetFlexObjects(InputStream is, OutputStream os) throws Exception {
        try {
            List<String> names = new Vector();
            while (true) {
                try {
                    String name = CommonTools.readString(is);
                    if (name == null) {
                        break;
                    }
                    names.add(name);
                } catch (Exception e) {
                    e.printStackTrace();
                    break;
                }
            }

            // logger.warning("doGetFlexObjects:" + names.toString());
            List<FlexObjectEntry> contents = this.flexObjectFactory.getFlexObjects(names);
            // logger.warning("doGetFlexObjects finished:" + names.toString());
            CommonTools.writeString(os, "ok");
            for (FlexObjectEntry entry : contents) {
                if (entry == null) {
                    CommonTools.writeEntry(os, FlexObjectEntry.empty);
                } else {
                    CommonTools.writeEntry(os, entry);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void stop() {
        try {
            flexObjectFactory.stop();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "can not stop flexobjectfactory", e);
        }
    }

    public void set_state_word(int state_word) throws Exception {
        try {
            flexObjectFactory.set_state_word(state_word);
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "can not set_state_word flexobjectfactory", e);
        }
    }
}
