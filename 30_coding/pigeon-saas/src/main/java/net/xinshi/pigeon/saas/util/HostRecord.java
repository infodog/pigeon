package net.xinshi.pigeon.saas.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-9-25
 * Time: 下午3:29
 * To change this template use File | Settings | File Templates.
 */

public class HostRecord {

    String host;
    long times;
    long bytes_in;
    long bytes_out;

    public HostRecord(String host, long times, long bytes_in, long bytes_out) {
        this.host = host;
        this.times = times;
        this.bytes_in = bytes_in;
        this.bytes_out = bytes_out;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public long getTimes() {
        return times;
    }

    public void setTimes(long times) {
        this.times = times;
    }

    public long getBytes_in() {
        return bytes_in;
    }

    public void setBytes_in(long bytes_in) {
        this.bytes_in = bytes_in;
    }

    public long getBytes_out() {
        return bytes_out;
    }

    public void setBytes_out(long bytes_out) {
        this.bytes_out = bytes_out;
    }

    public static synchronized List<HostRecord> handle(String tools) throws Exception {
        String command = tools + " > /tmp/saas_log ";
        try {
            Runtime.getRuntime().exec(command);
        } catch (Exception ex) {
            System.err.println(ex.toString());
            throw ex;
        }
        List<HostRecord> listHosts = new ArrayList<HostRecord>();
        BufferedReader br = new BufferedReader(new FileReader(new File("/tmp/saas_log")));
        String line;
        while (true) {
            if ((line = br.readLine()) == null) {
                break;
            }
            String[] parts = line.split(",");
            if (parts.length != 4) {
                continue;
            }
            HostRecord hr = new HostRecord(parts[0].trim(), Long.valueOf(parts[1].trim()), Long.valueOf(parts[2].trim()), Long.valueOf(parts[3].trim()));
            listHosts.add(hr);
        }
        return listHosts;
    }

}
