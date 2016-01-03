package net.xinshi.pigeon.saas.util;

import net.xinshi.pigeon.adapter.StaticPigeonEngine;
import net.xinshi.pigeon.list.ISortList;
import net.xinshi.pigeon.list.SortListObject;
import net.xinshi.pigeon.saas.SaasPigeonEngine;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-8-24
 * Time: 上午9:57
 * To change this template use File | Settings | File Templates.
 */

public class MerchantIdManager {

    public static void addMerchantId(String MerchantId, String MerchantDomain) throws Exception {
        String mid = ((SaasPigeonEngine) StaticPigeonEngine.pigeon).getCurrentMerchantId();
        ((SaasPigeonEngine) StaticPigeonEngine.pigeon).setCurrentMerchantId("");
        try {
            StaticPigeonEngine.pigeon.getLock().Lock("MerchantIdManager");
            ISortList merchantList = StaticPigeonEngine.pigeon.getListFactory().getList("SaasMerchantList", true);
            List<SortListObject> listObjects = merchantList.getRange(0, -1);
            for (SortListObject slo : listObjects) {
                String key = slo.getKey();
                if (key == null || org.apache.commons.lang.StringUtils.isBlank(key)) {
                    continue;
                }
                if (key.equals(MerchantId)) {
                    throw new Exception("MerchantId : " + MerchantId + ", exist!");
                }
            }
            String domain = StaticPigeonEngine.pigeon.getFlexObjectFactory().getContent(MerchantDomain);
            if (domain != null && !org.apache.commons.lang.StringUtils.isBlank(domain)) {
                throw new Exception("MerchantDomain : " + MerchantDomain + ", exist! MerchantId = " + domain);
            }
            StaticPigeonEngine.pigeon.getFlexObjectFactory().addContent(MerchantDomain, MerchantId);
            merchantList.add(new SortListObject(MerchantId, MerchantDomain));
        } finally {
            StaticPigeonEngine.pigeon.getLock().Unlock("MerchantIdManager");
            ((SaasPigeonEngine) StaticPigeonEngine.pigeon).setCurrentMerchantId(mid);
        }
    }

}

