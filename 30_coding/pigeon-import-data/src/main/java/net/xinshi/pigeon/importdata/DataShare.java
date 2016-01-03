package net.xinshi.pigeon.importdata;

import net.xinshi.pigeon.persistence.VersionHistory;
import net.xinshi.pigeon.persistence.VersionHistoryLogger;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-6-11
 * Time: 上午10:31
 * To change this template use File | Settings | File Templates.
 */

public class DataShare {
    config config;
    VersionHistoryLogger versionHistoryLogger;
    List<String> allFiles = null;
    FileInputStream fis = null;

    public DataShare(config config) {
        this.config = config;
    }

    public void init() throws Exception{
        versionHistoryLogger = new VersionHistoryLogger();
        versionHistoryLogger.setLoggerDirectory(config.DataDir);
        allFiles = versionHistoryLogger.getAllLoggerFile();
    }

    private FileInputStream cursorFIS() throws Exception {
        while (true) {
            if (allFiles == null || allFiles.size() == 0) {
                return null;
            }
            String file = allFiles.get(0);
            file = config.DataDir + "/" + file;
            allFiles.remove(0);
            long minmax[] = versionHistoryLogger.getMinMaxVersion(file);
            if (minmax.length != 2) {
                throw new Exception(file + " : log file bad minmax version ... ");
            }
            System.out.println(file + " : min = " + minmax[0] + ", max = " + minmax[1]);
            if (minmax[1] < config.beginVersion) {
                System.out.println(file + " : max = " + minmax[1] + " < " + config.beginVersion + " skip ... ");
                continue;
            }
            if (config.endVersion != -1 && minmax[0] > config.endVersion) {
                System.out.println(file + " : min = " + minmax[0] + " > " + config.endVersion + " over ... ");
                return null;
            }
            FileInputStream is = new FileInputStream(new File(file));
            return is;
        }
    }

    public VersionHistory fetchVersionHistory() throws Exception {
        while (true) {
            if (fis == null) {
                fis = cursorFIS();
            }
            if (fis == null) {
                return null;
            }
            VersionHistory vh = VersionHistoryLogger.getVersionHistoryFromFIS(fis);
            if (vh == null) {
                fis = null;
                continue;
            }
            if (vh.getVersion() < config.beginVersion) {
                continue;
            }
            if (config.endVersion != -1 && vh.getVersion() > config.endVersion) {
                return null;
            }
            return vh;
        }
    }

}

