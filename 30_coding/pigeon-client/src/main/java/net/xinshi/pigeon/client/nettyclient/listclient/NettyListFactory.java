package net.xinshi.pigeon.client.nettyclient.listclient;

import net.xinshi.pigeon.list.IListFactory;
import net.xinshi.pigeon.list.ISortList;
import net.xinshi.pigeon.netty.client.Client;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-1-31
 * Time: 上午10:07
 * To change this template use File | Settings | File Templates.
 */

public class NettyListFactory implements IListFactory {

    String host = "127.0.0.1";

    int port = 8878;

    Client ch = null;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    String url;

    public ISortList getList(String listId, boolean create) throws Exception {
        NettySortList sortList = new NettySortList();
        sortList.setListId(listId);
        sortList.setNettyClient(ch);
        sortList.setUrl(url);
        return sortList;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public ISortList createList(String listId) throws Exception {
        throw new Exception("not implemented.");
    }

    public void init() throws Exception {
        ch = new Client(host, port, 10);
        boolean rc = ch.init();
        if (!rc) {
            System.out.println("list init netty client failed!!!!!!!!");
        }
    }

    public void set_state_word(int state_word) throws Exception {
    }
}

