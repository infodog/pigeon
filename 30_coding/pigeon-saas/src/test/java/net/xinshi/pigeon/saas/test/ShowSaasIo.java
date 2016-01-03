package net.xinshi.pigeon.saas.test;

import net.xinshi.pigeon.saas.util.HostRecord;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-9-25
 * Time: 下午3:28
 * To change this template use File | Settings | File Templates.
 */

public class ShowSaasIo {

    public static void main(String[] args) throws Exception {
        List<HostRecord> listHosts = HostRecord.handle("/data/httpd-2.4.2/saas_all/show_saas_io");
        for (HostRecord hr : listHosts) {
            System.out.println("Host:" + hr.getHost() + ", count:" + hr.getTimes() + ", RX bytes:" + hr.getBytes_in() + ", TX bytes:" + hr.getBytes_out());
        }
    }

}

