package net.xinshi.pigeon.saas.util;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.adapter.StaticPigeonEngine;
//import net.xinshi.pigeon.dumpload.loaddata.PigeonLoad;
import net.xinshi.pigeon.saas.SaasPigeonEngine;
import net.xinshi.pigeon.saas.adapter.SaasPigeonStoreEngine;

import java.net.URL;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-8-24
 * Time: 上午10:51
 * To change this template use File | Settings | File Templates.
 */
@Deprecated
public class InitMerchant {

    public static void init(String MerchantId, String DataDir) throws Exception {
        if (MerchantId == null || org.apache.commons.lang.StringUtils.isBlank(MerchantId)) {
            return;
        }
        String mid = ((SaasPigeonEngine) StaticPigeonEngine.pigeon).getCurrentMerchantId();
        try {
            String path = DataDir;
            if (path.startsWith("/") || path.charAt(1) == ':') {
                // absolute path
            } else {
                String pigeon_config = "pigeonclient.conf";
                URL url = InitMerchant.class.getClassLoader().getResource(pigeon_config);
                path = url.getPath();
                path = path.substring(0, path.length() - pigeon_config.length()) + DataDir;
            }
            System.out.println("init merchant : id = " + MerchantId + ", DataDir = " + path);
            SaasPigeonStoreEngine saasPigeonStoreEngine = new SaasPigeonStoreEngine();
            ((SaasPigeonEngine) StaticPigeonEngine.pigeon).setCurrentMerchantId(MerchantId);
            saasPigeonStoreEngine.setAtom(((SaasPigeonEngine) StaticPigeonEngine.pigeon).getAtom());
            saasPigeonStoreEngine.setFlexObjectFactory(((SaasPigeonEngine) StaticPigeonEngine.pigeon).getFlexObjectFactory());
            saasPigeonStoreEngine.setIdGenerator(((SaasPigeonEngine) StaticPigeonEngine.pigeon).getIdGenerator());
            saasPigeonStoreEngine.setListFactory(((SaasPigeonEngine) StaticPigeonEngine.pigeon).getListFactory());
            saasPigeonStoreEngine.setResourceLock(((SaasPigeonEngine) StaticPigeonEngine.pigeon).getLock());
            //PigeonLoad.doit(saasPigeonStoreEngine, path);
        } finally {
            ((SaasPigeonEngine) StaticPigeonEngine.pigeon).setCurrentMerchantId(mid);
        }
    }

    public static void init(String MerchantId, String DataDir, IPigeonStoreEngine pigeonStoreEngine) throws Exception {
        if (MerchantId == null || org.apache.commons.lang.StringUtils.isBlank(MerchantId)) {
            return;
        }
        if (!(pigeonStoreEngine instanceof SaasPigeonEngine)) {
            throw new Exception("!(pigeonStoreEngine instanceof SaasPigeonEngine)");
        }
        String mid = ((SaasPigeonEngine) pigeonStoreEngine).getCurrentMerchantId();
        try {
            String path = DataDir;
            if (path.startsWith("/") || path.charAt(1) == ':') {
                // absolute path
            } else {
                String pigeon_config = "pigeonclient.conf";
                URL url = InitMerchant.class.getClassLoader().getResource(pigeon_config);
                path = url.getPath();
                path = path.substring(0, path.length() - pigeon_config.length()) + DataDir;
            }
            System.out.println("init merchant : id = " + MerchantId + ", DataDir = " + path);
            SaasPigeonStoreEngine saasPigeonStoreEngine = new SaasPigeonStoreEngine();
            ((SaasPigeonEngine) pigeonStoreEngine).setCurrentMerchantId(MerchantId);
            saasPigeonStoreEngine.setAtom(pigeonStoreEngine.getAtom());
            saasPigeonStoreEngine.setFlexObjectFactory(pigeonStoreEngine.getFlexObjectFactory());
            saasPigeonStoreEngine.setIdGenerator(pigeonStoreEngine.getIdGenerator());
            saasPigeonStoreEngine.setListFactory(pigeonStoreEngine.getListFactory());
            saasPigeonStoreEngine.setResourceLock(pigeonStoreEngine.getLock());
            //PigeonLoad.doit(saasPigeonStoreEngine, path);
        } finally {
            ((SaasPigeonEngine) pigeonStoreEngine).setCurrentMerchantId(mid);
        }
    }

}
