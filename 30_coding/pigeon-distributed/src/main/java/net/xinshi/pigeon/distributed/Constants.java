package net.xinshi.pigeon.distributed;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-3
 * Time: 上午11:20
 * To change this template use File | Settings | File Templates.
 */

public class Constants {

    public static final String version = "3.1.1(2015.12.20)";

    public static final int DB_KEY_MAX_LENGTH = 500;
    public static final int DB_NAME_MAX_LENGTH = 125;

    public static final int PACKET_PREFIX_LENGTH = 10;
    public static final int CONTROL_SERVER_PORT = 3309;
    public static final String CONFIG_FILE = "../conf/pigeonnodes.conf";
    public static final int HEARTBEAT_TIMEOUT = 60;

    public static final int ACTION_LOGIN = 0xA;
    public static final int ACTION_HEARTBEAT = 0xB;
    public static final int ACTION_NODES_INFO = 0xC;

    public static final int NORMAL_STATE = 0x0;
    public static final int NOWRITEDB_STATE = 0x1;
    public static final int READONLY_STATE = 0x2;
    public static final int STOP_STATE = 0x3;

    public static boolean canWriteLog(int state) {
        return state <= NOWRITEDB_STATE;
    }

    public static boolean canWriteDB(int state) {
        return state == NORMAL_STATE;
    }

    public static boolean isStop(int state) {
        return state == STOP_STATE;
    }

    public static boolean isReadOnly(int state) {
        return state == READONLY_STATE;
    }

    public static boolean isAvailable(int state) {
        return state >= NORMAL_STATE && state <= STOP_STATE;
    }

}

