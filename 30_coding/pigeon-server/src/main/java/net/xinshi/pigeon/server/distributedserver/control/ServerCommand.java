package net.xinshi.pigeon.server.distributedserver.control;

import net.xinshi.pigeon.netty.client.Client;
import net.xinshi.pigeon.netty.common.PigeonFuture;
import net.xinshi.pigeon.server.distributedserver.Constants;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午2:14
 * To change this template use File | Settings | File Templates.
 */

public class ServerCommand {

    String command;
    String host;
    String port;

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public ServerCommand(String command, String host, String port) {
        this.command = command;
        this.host = host;
        this.port = port;
    }

    public void invoke_cmd() {
        try {
            long state_word = -1;
            if (command.compareTo("normal") == 0) {
                state_word = net.xinshi.pigeon.netty.common.Constants.CONTROL_MAGIC_NORMAL;
            } else if (command.compareTo("nowritedb") == 0) {
                state_word = net.xinshi.pigeon.netty.common.Constants.CONTROL_MAGIC_NOWRITEDB;
            } else if (command.compareTo("readonly") == 0) {
                state_word = net.xinshi.pigeon.netty.common.Constants.CONTROL_MAGIC_READONLY;
            } else if (command.compareTo("stop") == 0) {
                state_word = net.xinshi.pigeon.netty.common.Constants.CONTROL_MAGIC_STOP;
            } else {
                System.out.println("command error!");
                return;
            }
            System.out.println(host + ":" + port + " do [" + command + "]");
            Client ch = new Client(host, Integer.parseInt(port), 1);
            boolean rc = ch.init();
            if (!rc) {
                System.out.println("connect to server failed");
                return;
            }
            int len = 2 + 8;
            ChannelBuffer dcb = ChannelBuffers.dynamicBuffer(len);
            dcb.writeShort(net.xinshi.pigeon.netty.common.Constants.CONTROL_TYPE);
            dcb.writeLong(state_word);
            PigeonFuture pf = ch.send(dcb.array());
            String info = "wait time out ...... ";
            if (pf != null && pf.waitme(1000 * 60)) {
                info = new String(pf.getData(), Constants.PACKET_PREFIX_LENGTH, pf.getData().length - Constants.PACKET_PREFIX_LENGTH, "UTF-8");
            }
            System.out.println(info);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

