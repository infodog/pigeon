package net.xinshi.pigeon.distributed.client;

import net.xinshi.pigeon.netty.client.Client;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-5
 * Time: 下午2:28
 * To change this template use File | Settings | File Templates.
 */

public class ClientNode implements Comparable<ClientNode> {

    short type;
    short no;
    Client connection;

    public ClientNode(short type, short no, Client connection) {
        this.type = type;
        this.no = no;
        this.connection = connection;
    }

    public short getType() {
        return type;
    }

    public void setType(short type) {
        this.type = type;
    }

    public short getNo() {
        return no;
    }

    public void setNo(short no) {
        this.no = no;
    }

    public Client getConnection() {
        return connection;
    }

    public void setConnection(Client connection) {
        this.connection = connection;
    }

    public int compareTo(ClientNode o) {
        return connection.compareTo(o.getConnection());
    }

}

