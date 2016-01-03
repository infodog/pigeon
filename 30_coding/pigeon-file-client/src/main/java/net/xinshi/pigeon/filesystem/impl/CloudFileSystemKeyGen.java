package net.xinshi.pigeon.filesystem.impl;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * Created by IntelliJ IDEA.
 * User: kindason
 * Date: 2010-9-20
 * Time: 10:40:55
 * To change this template use File | Settings | File Templates.
 */

public class CloudFileSystemKeyGen {
    /**
     * @param fileName 前端传入的文件名称，格式为:xxx.jpg,不能带任何的‘/’
     * @return 格式：lg1_server1@2010/10/10/23423.jpg
     * @throws Exception
     */
    public static String genFileKey(String fileName) throws Exception {
        if (fileName.contains("/"))
            throw new Exception("Wrong format: '/'");
        StringBuffer buff = new StringBuffer();
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(new Date());
        buff.append(cal.get(Calendar.YEAR))
                .append("/").append(cal.get(Calendar.MONTH) + 1)
                .append("/").append(cal.get(Calendar.DAY_OF_MONTH))
                .append("/").append(fileName);

        return buff.toString();
    }
}
