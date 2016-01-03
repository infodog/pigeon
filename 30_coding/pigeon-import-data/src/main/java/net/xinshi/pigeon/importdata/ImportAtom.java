package net.xinshi.pigeon.importdata;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.persistence.VersionHistory;
import net.xinshi.pigeon.util.TimeTools;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-6-11
 * Time: 下午2:46
 * To change this template use File | Settings | File Templates.
 */

public class ImportAtom {
    DataShare ds;
    IPigeonStoreEngine pigeonStoreEngine;
    public long lastVersion = -1;

    public ImportAtom(config config, IPigeonStoreEngine pigeonStoreEngine) {
        this.pigeonStoreEngine = pigeonStoreEngine;
        ds = new DataShare(config);
    }

    public void init() throws Exception {
        ds.init();
    }

    public long ImportDataFiles() throws Exception {
        int n = 0;
        while (true) {
            VersionHistory vh = ds.fetchVersionHistory();
            if (vh == null) {
                break;
            }
            if (vh.getVersion() % 10000 == 0) {
                System.out.println(TimeTools.getNowTimeString() + " Atom import version = " + vh.getVersion());
            }
            String line = new String(vh.getData(), "UTF-8");
            line = line.trim();
            line.replace("\n", "");
            String[] parts = line.split(" ");
            String op = parts[0];
            String name = parts[1];
            if (op.equals("createAndSet")) {
                String value = parts[2];
                long lvalue = Long.parseLong(value);
                while (true) {
                    try {
                        pigeonStoreEngine.getAtom().createAndSet(name, Long.valueOf(lvalue).intValue());
                        ++n;
                        break;
                    } catch (Exception e) {
                        e.printStackTrace();
                        Thread.sleep(1000);
                    }
                }
            } else if (op.equals("greaterAndInc")) {
                String value = parts[2];
                long testvalue = Long.parseLong(value);
                long incvalue = Long.parseLong(parts[3]);
                while (true) {
                    try {
                        pigeonStoreEngine.getAtom().greaterAndInc(name, Long.valueOf(testvalue).intValue(), Long.valueOf(incvalue).intValue());
                        ++n;
                        break;
                    } catch (Exception e) {
                        if (e.getMessage().indexOf("does not exists!") >= 0) {
                            System.out.println("atom name = " + name + ", does not exists! skip !!! ");
                            break;
                        }
                        e.printStackTrace();
                        Thread.sleep(1000);
                    }
                }
            } else if (op.equals("lessAndInc")) {
                String value = parts[2];
                long testvalue = Long.parseLong(value);
                long incvalue = Long.parseLong(parts[3]);
                while (true) {
                    try {
                        pigeonStoreEngine.getAtom().lessAndInc(name, Long.valueOf(testvalue).intValue(), Long.valueOf(incvalue).intValue());
                        ++n;
                        break;
                    } catch (Exception e) {
                        if (e.getMessage().indexOf("does not exists!") >= 0) {
                            System.out.println("atom name = " + name + ", does not exists! skip !!! ");
                            break;
                        }
                        e.printStackTrace();
                        Thread.sleep(1000);
                    }
                }
            } else {
                throw new Exception("import atom not correct log format:" + line + ", version = " + vh.getVersion());
            }
            lastVersion = vh.getVersion();
        }
        return n;
    }

}

