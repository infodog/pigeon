package net.xinshi.pigeon.tags.utils;

import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 * User: mac
 * Date: 12-1-21
 * Time: 下午6:00
 * To change this template use File | Settings | File Templates.
 */
public class ConfigHelper {

    static Properties configProperties = null;
    static boolean initialized = false;

    synchronized  static void init(){
        if(initialized) return;
        initialized = true;
        configProperties = new Properties();
        try {
            configProperties.load(ConfigHelper.class.getResourceAsStream("/pigeonTags.properties"));
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public static String getConfigProperty(String key){
      if(!initialized){
          init();
      }
      if(configProperties==null) return "";
      String value = configProperties.getProperty(key);
      if(value==null) return "";
      return value;
    }
}
