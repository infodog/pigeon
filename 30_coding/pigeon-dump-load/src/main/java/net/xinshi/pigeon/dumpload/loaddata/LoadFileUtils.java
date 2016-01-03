package net.xinshi.pigeon.dumpload.loaddata;

import java.io.*;

/**
 * Created by Administrator on 14-2-25.
 */
public class LoadFileUtils {
    public static void saveLoadLog(String fileName,long line) throws IOException {
        String logFileName = fileName + ".log";
        File f = new File(logFileName);
        FileOutputStream os = new FileOutputStream(f,false);
        os.write(String.valueOf(line).getBytes("utf-8"));
        os.close();
    }

    public static long getLoadLog(String fileName) throws IOException {
        String logFileName = fileName + ".log";
        File f = new File(logFileName);
        try{
            FileInputStream is = new FileInputStream(f);
            byte[] buf = new byte[2048];
            int n = is.read(buf);
            if(n!=-1){
                String s = new String(buf,0,n);
                return Long.parseLong(s);
            }
            return 0;
        }
        catch (Exception e){
            return 0;
        }
        //return 0;
    }
}
