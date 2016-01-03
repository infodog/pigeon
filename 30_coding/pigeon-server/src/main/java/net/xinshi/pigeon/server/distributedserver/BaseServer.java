package net.xinshi.pigeon.server.distributedserver;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 上午11:45
 * To change this template use File | Settings | File Templates.
 */

public class BaseServer implements IServer {

    String type;
    String nodeName;
    String instanceName;
    char role;
    String nodesString;
    static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String startTime;
    protected long writes = 0;
    protected long reads = 0;
    protected long version = 0;

    public BaseServer() {
        startTime = this.dateFormat.format(new Date());
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public String getStartTime() {
        return startTime;
    }

    public char getRole() {
        return role;
    }

    public void setRole(char role) {
        this.role = role;
    }

    public String getNodesString() {
        return nodesString;
    }

    public void setNodesString(String nodesString) {
        this.nodesString = nodesString;
    }

    public void start() throws Exception {
    }

    public void stop() throws Exception {
    }

}

