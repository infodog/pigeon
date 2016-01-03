package net.xinshi.pigeon.persistence;

import net.xinshi.pigeon.atom.impls.dbatom.FastAtom;
import net.xinshi.pigeon.distributed.duplicate.DuplicateService;
import net.xinshi.pigeon.flexobject.impls.fastsimple.SimpleFlexObjectFactory;
import net.xinshi.pigeon.idgenerator.impl.MysqlIDGenerator;
import net.xinshi.pigeon.list.bandlist.SortBandListFactory;
import net.xinshi.pigeon.util.TimeTools;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-4-1
 * Time: 上午9:29
 * To change this template use File | Settings | File Templates.
 */

public class PersistenceService {

    public static void syncVersionTask(IPigeonPersistence ipp) {
        if(DuplicateService.getDuplicateConfig()==null){
            return;
        }
        try {
            String type = "";
            if (ipp instanceof FastAtom) {
                type = "Atom";
            } else if (ipp instanceof SimpleFlexObjectFactory) {
                type = "FlexObject";
            } else if (ipp instanceof SortBandListFactory) {
                type = "List";
            } else if (ipp instanceof MysqlIDGenerator) {
                type = "IdServer";
            }
            final String Type = type;
            while (true) {
                try {
                    long maxVer = DuplicateService.masterVersion(ipp);
                    if (maxVer != -1) {
                        System.out.println(TimeTools.getNowTimeString() + " " + type + " my version = " + ipp.getVersion() + " master version = " + maxVer);
                    }
                    if (ipp.getVersion() < maxVer) {
                        ipp.syncVersion(ipp.getVersion() + 1, maxVer);
                    }
                    break;
                } catch (Exception e) {
                    e.printStackTrace();
                    try {
                        Thread.sleep(1000 * 5);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            }
            final IPigeonPersistence that = ipp;
            new Thread(new Runnable() {
                public void run() {
                    long lastTime = 0;
                    long lastVer = -1;
                    String lastStr = "";
                    Thread.currentThread().setName("PersistenceService_run");
                    while (true) {
                        try {
                            long maxVer = DuplicateService.masterVersion(that);
                            if (maxVer < 0) {
                                System.out.println(TimeTools.getNowTimeString() + " " + Type + " no master node, syncVersion() thread return. maxVer = " + maxVer);
                                return;
                            }
                            long curVer = that.getVersion();
                            if (curVer == lastVer && curVer < maxVer) {
                                System.out.println(TimeTools.getNowTimeString() + " " + Type + " task : my version = " + curVer + " master version = " + maxVer);
                                that.syncVersion(curVer, maxVer);
                            } else if (System.currentTimeMillis() > lastTime + 1000 * 600) {
                                String verString = Type + " lastVer = " + lastVer + ", curVer = " + curVer + ", maxVer = " + maxVer;
                                if (lastStr.compareTo(verString) != 0) {
                                    System.out.println(TimeTools.getNowTimeString() + " " + verString);
                                    lastStr = verString;
                                }
                                lastTime = System.currentTimeMillis();
                            }
                            lastVer = curVer;
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        try {
                            Thread.sleep(1000 * 60);
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                }
            }).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

