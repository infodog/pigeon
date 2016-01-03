package net.xinshi.pigeon.dumpload.loaddata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-5-11
 * Time: 下午4:00
 * To change this template use File | Settings | File Templates.
 */

public class LoadDataFiles extends Thread {
    int ignore = 0;
    int total = 0;
    String type;
    Vector<String> files;
    boolean bHex = false;
    LinkedBlockingQueue<HexRecord> queKVs = new LinkedBlockingQueue<HexRecord>();
    Logger logger = Logger.getLogger(LoadDataFiles.class.getName());

    public LoadDataFiles(String type, Vector<String> files, boolean hex) {
        this.type = type;
        this.files = files;
        this.bHex = hex;
    }

    public void init() throws Exception {
        for (String fileName : files) {
            File f = new File(fileName);
            if (!f.exists() || f.length() == 0) {
                throw new Exception(fileName + " is bad file ...");
            }
        }
        ReadFile rf = new ReadFile();
        rf.start();
    }

    public HexRecord fetchHexRecord() throws Exception {
        HexRecord hr = queKVs.take();
        return hr;
    }

    private class ReadFile extends Thread {
        public void run() {
            String threadid = type + " " + Thread.currentThread().getName() + " ";
            for (int i = 0; i < files.size(); i++) {
                try {
                    File f = new File(files.get(i));
                    BufferedReader br = new BufferedReader(new FileReader(f));
                    long lastLine = LoadFileUtils.getLoadLog(f.getAbsolutePath());
                    String line;
                    int count = 0;
                    while (true) {
                        if (queKVs.size() < 5000) {
                            try {
                                if ((line = br.readLine()) == null) {
                                    break;
                                }
                                count++;
                                if(count<lastLine){
                                    continue;
                                }
                                line.replace("\r", "");
                                line.replace("\n", "");
                                String[] parts = line.split("\t");
                                if (parts.length != 2) {
                                    System.out.println(threadid + "warnning lost value : line = " + line);
                                    ++ignore;
                                    continue;
                                }
                                String name = hexToString(parts[0]);
                                String value = parts[1];
                                if (bHex) {
                                    value = hexToString(parts[1]);
                                }
                                HexRecord hr = new HexRecord(name, value);
                                if(count%1000==0){
                                    hr.setFileName(f.getAbsolutePath());
                                    hr.setCurLine(count);
                                    logger.info(f.getAbsolutePath()+"," + count);

                                }
                                queKVs.add(hr);

                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        } else {
                            try {
                                Thread.sleep(1000);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    System.out.println(files.get(i) + " load data finished. count = " + count);
                    total += count;
                    if (br != null) {
                        br.close();
                        br = null;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            for (int i = 0; i < 1000; i++) {
                synchronized (queKVs) {
                    queKVs.add(new HexRecord(null, null));
                }
            }
            System.out.println(type + " load data finished. count = " + total + ", ignore = " + ignore);
        }
    }

    static String hexToString(String strValue) throws Exception {
        int len = strValue.length();
        if (len == 0) {
            return "";
        }
        if (len % 2 != 0) {
            throw new Exception("bad hex string : " + strValue);
        }
        byte buff[] = strValue.getBytes();
        byte byteData[] = new byte[len / 2];
        int j = 0;
        for (int i = 0; i < len; i += 2) {
            byte b1 = buff[i];
            byte b2 = buff[i + 1];
            if (b1 >= '0' && b1 <= '9') {
                b1 -= '0';
            } else if (b1 >= 'a' && b1 <= 'f') {
                b1 -= 'a' - 10;
            } else if (b1 >= 'A' && b1 <= 'F') {
                b1 -= 'A' - 10;
            } else {
                throw new Exception("bad hex string : " + strValue);
            }
            if (b2 >= '0' && b2 <= '9') {
                b2 -= '0';
            } else if (b2 >= 'a' && b2 <= 'f') {
                b2 -= 'a' - 10;
            } else if (b2 >= 'A' && b2 <= 'F') {
                b2 -= 'A' - 10;
            } else {
                throw new Exception("bad hex string : " + strValue);
            }
            byteData[j++] = (byte) (b1 << 4 | b2);
        }
        return new String(byteData, "UTF-8");
    }

}
