package net.xinshi.pigeon.config;

import net.xinshi.pigeon.distributed.bean.HashRange;
import net.xinshi.pigeon.distributed.bean.PigeonNode;
import net.xinshi.pigeon.distributed.manager.PigeonNodesManager;
import org.apache.commons.beanutils.BeanUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: WPF
 * Date: 13-8-9
 * Time: 下午3:09
 * To change this template use File | Settings | File Templates.
 */

public class MakeConfig {
    static long LEFT_BOUNDARY = Integer.MIN_VALUE;
    static long RIGHT_BOUNDARY = Integer.MAX_VALUE;

    public static String MakeFileSystem(String host, String port) throws Exception {
        String info = template.pigeonfilesystem;

        info = info.replace("${host}", host);
        info = info.replace("${port}", String.valueOf(Integer.parseInt(port) + 1));

        return info;
    }

    public static String MakeServer(String host, String port, String no, String dbname, String dbuser, String dbpasswd, boolean filesystem) throws Exception {
        String server = template.pigeonserver;

        server = server.replace("${host}", host);
        server = server.replace("${port}", port);
        server = server.replace("${no}", no);
        server = server.replace("${ver}", version);
        server = server.replace("${dbname}", dbname);
        server = server.replace("${dbuser}", dbuser);
        server = server.replace("${dbpasswd}", dbpasswd);
        if (!filesystem) {
            int p1 = server.lastIndexOf("},");
            if (p1 > 0) {
                int p2 = server.indexOf("}", p1 + 1);
                if (p2 > 0) {
                    String t = server.substring(p1, p2);
                    server = server.replace(t, "");
                }
            }
        }

        return server;
    }

    private static String[] types = {"flexobject", "list", "atom", "idserver", "lock"};
    private static String[] roles = {"M", "S", "B"};
    private static String version = "0";

    private static String fetchString(String[] array, int no) {
        return array[no % array.length];
    }

    private static List<Map> fetchMembers(String type, String[] hosts, String[] ports, int copys, int pos) throws Exception {
        List<Map> members = new ArrayList<Map>();
        for (int j = 0; j < copys; j++) {
            Map<String, String> member = new HashMap<String, String>();
            member.put("name", type + "1");
            member.put("host", fetchString(hosts, pos + j));
            member.put("port", fetchString(ports, j));
            member.put("role", fetchString(roles, j));
            members.add(member);
            if (type.equals("lock")) {
                break;
            }
        }
        return members;
    }

    private static String[] ReverseArray(String[] array) throws Exception {
        for (int i = 0; i < array.length / 2; i++) {
            String t = array[i];
            array[i] = array[array.length - i - 1];
            array[array.length - i - 1] = t;
        }
        return array;
    }

    public static String MakeNodes(String[] hosts, String[] ports, int copys) throws Exception {
        if (hosts.length < copys) {
            throw new Exception("MakeNodes count of hosts < copys");
        }
        if (copys < 0 || copys > 3) {
            throw new Exception("MakeNodes copys < 0 || copys > 3");
        }
        if (ports.length != copys) {
            throw new Exception("MakeNodes count of ports != copys");
        }
        java.util.Arrays.sort(hosts);
        java.util.Arrays.sort(ports);
        ReverseArray(ports);
        List<String> ranges = new ArrayList<String>();
        long delta = (RIGHT_BOUNDARY - LEFT_BOUNDARY + hosts.length - 1) / hosts.length;
        long cur = LEFT_BOUNDARY;
        while (cur < RIGHT_BOUNDARY) {
            long end = cur + delta;
            if (end > RIGHT_BOUNDARY) {
                end = RIGHT_BOUNDARY;
            }
            ranges.add(String.format("%d~%d", cur, end));
            cur = end + 1;
        }
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("version", version);
        JSONArray jsonArray = new JSONArray();
        for (int i = 0; i < types.length; i++) {
            JSONObject jsonT = new JSONObject();
            jsonT.put("type", types[i]);
            JSONArray segments = new JSONArray();
            for (int j = 0; j < ranges.size(); j++) {
                JSONObject jsonN = new JSONObject();
                jsonN.put("range", ranges.get(j));
                jsonN.put("members", fetchMembers(types[i], hosts, ports, copys, j));
                segments.put(jsonN);
            }
            jsonT.put("segments", segments);
            jsonArray.put(jsonT);
        }
        jsonObject.put("pigeon_nodes", jsonArray);
        return jsonObject.toString(8);
    }

    private static void test() throws Exception {

        String[] hosts = {"1.1.1.1", "2.2.2.2", "3.3.3.3"};
        String[] ports = {"8878", "8868", "8858"};
        String temp = MakeNodes(hosts, ports, 3);
        System.out.println(temp);
        temp = MakeNodes(Arrays.copyOf(hosts, 2), Arrays.copyOf(ports, 2), 2);
        System.out.println(temp);
        temp = MakeNodes(Arrays.copyOf(hosts, 1), Arrays.copyOf(ports, 1), 1);
        System.out.println(temp);

    }

    private static String touchVersion() throws Exception {
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        return sdf.format(date.getTime());
    }

    private static void saveConfig(String filename, String data) throws Exception {
        FileOutputStream os = new FileOutputStream(filename);
        data = data.replace("\n", "\r\n");
        os.write(data.getBytes());
        os.close();
    }

    private static String[] expandRange(String range) throws Exception {
        String[] lr = range.split("~");
        if (lr.length != 2) {
            throw new Exception("bad range = " + range);
        }
        Long l = Long.parseLong(lr[0]);
        Long r = Long.parseLong(lr[1]);
        long half = (r - l + 1) / 2;
        String range1 = String.format("%d~%d", l, l + half);
        String range2 = String.format("%d~%d", l + half + 1, r);
        return new String[]{range1, range2};
    }

    private static HashMap<String, String> mapPigeonNodes = new HashMap<String, String>();

    private static void makePigeonNodes(PigeonNode pn, String[] dbnames, String dbuser, String dbpasswd) throws Exception {
        String id = pn.getHost() + "_" + pn.getPort() + "_" + String.valueOf(pn.getRole());

        if (mapPigeonNodes.get(id) != null) {
            return;
        }
        int i = -1;
        int index = -1;
        for (String s : roles) {
            ++i;
            if (s.equals(String.valueOf(pn.getRole()))) {
                index = i;
                break;
            }
        }
        if (index < 0) {
            throw new Exception("bad role = " + String.valueOf(pn.getRole()));
        }
        String dbname = fetchString(dbnames, index);
        String server = MakeServer(pn.getHost(), "" + pn.getPort(), "1", dbname, dbuser, dbpasswd, false);
        saveConfig(id + "_pigeonserver.conf", server);
        mapPigeonNodes.put(id, server);
    }

    public static void main(String[] args) throws Exception {
        version = touchVersion();

        // test();

        if (args.length < 1) {
            System.out.println("MakeConfig {create|expand}");
            return;
        }
        if (args[0].equals("create")) {
            if (args.length < 2) {
                System.out.println("MakeConfig create {1|2|3}");
                return;
            }
            if (args[1].equals("1")) {
                // MakeConfig create 1 host port no dbname dbuser dbpasswd
                if (args.length != 7) {
                    System.out.println("MakeConfig create 1 host port dbname dbuser dbpasswd");
                    return;
                }
                String host = args[2];
                String port = args[3];
                String dbname = args[4];
                String dbuser = args[5];
                String dbpasswd = args[6];
                String server = MakeServer(host, port, "1", dbname, dbuser, dbpasswd, true);
                saveConfig(host + "_" + port + "_pigeonserver.conf", server);
                String nodes = MakeNodes(new String[]{host}, new String[]{port}, 1);
                saveConfig(host + "_" + port + "_pigeonnodes.conf", nodes);
                String filesystem = MakeFileSystem(host, port);
                saveConfig(host + "_" + String.valueOf(Integer.parseInt(port) + 1) + "_pigeonfilesystem.conf", filesystem);
            } else if (args[1].equals("2")) {
                // MakeConfig create 2 host1 host2 port1 port2 dbname1 dbname2 dbuser dbpasswd
                if (args.length != 10) {
                    System.out.println("MakeConfig create 2 host1 host2 port1 port2 dbname1 dbname2 dbuser dbpasswd");
                    return;
                }
                String host1 = args[2];
                String host2 = args[3];
                String port1 = args[4];
                String port2 = args[5];
                String dbname1 = args[6];
                String dbname2 = args[7];
                String dbuser = args[8];
                String dbpasswd = args[9];
                String s1m = MakeServer(host1, port1, "1", dbname1, dbuser, dbpasswd, false);
                String s1s = MakeServer(host1, port2, "1", dbname2, dbuser, dbpasswd, false);
                String s2m = MakeServer(host2, port1, "1", dbname1, dbuser, dbpasswd, false);
                String s2s = MakeServer(host2, port2, "1", dbname2, dbuser, dbpasswd, false);
                saveConfig(host1 + "_" + port1 + "_master_pigeonserver.conf", s1m);
                saveConfig(host1 + "_" + port2 + "_sync_pigeonserver.conf", s1s);
                saveConfig(host2 + "_" + port1 + "_master_pigeonserver.conf", s2m);
                saveConfig(host2 + "_" + port2 + "_sync_pigeonserver.conf", s2s);
                String nodes = MakeNodes(new String[]{host1, host2}, new String[]{port1, port2}, 2);
                saveConfig(host1 + "_" + host2 + "_pigeonnodes.conf", nodes);
            } else if (args[1].equals("#3")) {
                // MakeConfig create 3 host1 host2 host3 port1 port2 port3 dbname1 dbname2 dbname3 dbuser dbpasswd
                if (args.length != 13) {
                    System.out.println("MakeConfig create 3 host1 host2 host3 port1 port2 port3 dbname1 dbname2 dbname3 dbuser dbpasswd");
                    return;
                }
                String host1 = args[2];
                String host2 = args[3];
                String host3 = args[4];
                String port1 = args[5];
                String port2 = args[6];
                String port3 = args[7];
                String dbname1 = args[8];
                String dbname2 = args[9];
                String dbname3 = args[10];
                String dbuser = args[11];
                String dbpasswd = args[12];
                String s1m = MakeServer(host1, port1, "1", dbname1, dbuser, dbpasswd, false);
                String s1s = MakeServer(host1, port2, "1", dbname2, dbuser, dbpasswd, false);
                String s1b = MakeServer(host1, port3, "1", dbname3, dbuser, dbpasswd, false);
                String s2m = MakeServer(host2, port1, "1", dbname1, dbuser, dbpasswd, false);
                String s2s = MakeServer(host2, port2, "1", dbname2, dbuser, dbpasswd, false);
                String s2b = MakeServer(host2, port3, "1", dbname3, dbuser, dbpasswd, false);
                String s3m = MakeServer(host3, port1, "1", dbname1, dbuser, dbpasswd, false);
                String s3s = MakeServer(host3, port2, "1", dbname2, dbuser, dbpasswd, false);
                String s3b = MakeServer(host3, port3, "1", dbname3, dbuser, dbpasswd, false);
                saveConfig(host1 + "_" + port1 + "_master_pigeonserver.conf", s1m);
                saveConfig(host1 + "_" + port2 + "_sync_pigeonserver.conf", s1s);
                saveConfig(host1 + "_" + port3 + "_backup_pigeonserver.conf", s1b);
                saveConfig(host2 + "_" + port1 + "_master_pigeonserver.conf", s2m);
                saveConfig(host2 + "_" + port2 + "_sync_pigeonserver.conf", s2s);
                saveConfig(host2 + "_" + port3 + "_backup_pigeonserver.conf", s2b);
                saveConfig(host3 + "_" + port1 + "_master_pigeonserver.conf", s3m);
                saveConfig(host3 + "_" + port2 + "_sync_pigeonserver.conf", s3s);
                saveConfig(host3 + "_" + port3 + "_backup_pigeonserver.conf", s3b);
                String nodes = MakeNodes(new String[]{host1, host2, host3}, new String[]{port1, port2, port3}, 3);
                saveConfig(host1 + "_" + host2 + "_" + host3 + "_pigeonnodes.conf", nodes);
            } else {
                // MakeConfig create N host1 host2 host3 ... hostN port1 port2 port3 dbname1 dbname2 dbname3 dbuser dbpasswd
                Integer num = Integer.parseInt(args[1]);
                if (num < 3 || num > 100) {
                    System.out.println("warning : server count N < 3 || N > 100");
                    System.out.println("MakeConfig create N host1 host2 host3 ... hostN port1 port2 port3 dbname1 dbname2 dbname3 dbuser dbpasswd");
                    return;
                }
                if (args.length != num + 10) {
                    System.out.println("MakeConfig create N host1 host2 host3 ... hostN port1 port2 port3 dbname1 dbname2 dbname3 dbuser dbpasswd");
                    return;
                }
                String[] hosts = new String[num];
                for (int i = 0; i < hosts.length; i++) {
                    hosts[i] = args[i + 2];
                }
                java.util.Arrays.sort(hosts);
                int n = hosts.length + 2;
                String[] ports = new String[3];
                ports[0] = args[n++];
                ports[1] = args[n++];
                ports[2] = args[n++];
                java.util.Arrays.sort(ports);
                ReverseArray(ports);
                String[] dbnames = new String[3];
                dbnames[0] = args[n++];
                dbnames[1] = args[n++];
                dbnames[2] = args[n++];
                String dbuser = args[n++];
                String dbpasswd = args[n++];
                for (int i = 0; i < hosts.length; i++) {
                    for (int j = 0; j < ports.length; j++) {
                        String host = fetchString(hosts, i);
                        String port = fetchString(ports, j);
                        String dbname = fetchString(dbnames, j);
                        String server = MakeServer(host, port, "1", dbname, dbuser, dbpasswd, false);
                        saveConfig(host + "_" + port + "_" + dbname + "_pigeonserver.conf", server);
                    }
                }
                String nodes = MakeNodes(hosts, ports, 3);
                saveConfig("" + hosts.length + "nodes_pigeonnodes.conf", nodes);
            }
        } else if (args[0].equals("expand")) {
            if (args.length < 5) {
                System.out.println("MakeConfig expand pigeonnodes.conf host1 host2 ... hostN dbuser dbpasswd");
                return;
            }
            PigeonNodesManager nodesMgr = new PigeonNodesManager();
            nodesMgr.init(args[1]);
            String[] hosts = new String[args.length - 4];
            for (int i = 0; i < hosts.length; i++) {
                hosts[i] = args[i + 2];
            }
            java.util.Arrays.sort(hosts);
            HashMap<String, List<HashRange>> mapRanges = new HashMap<String, java.util.List<HashRange>>();
            for (Map.Entry<String, List<HashRange>> entry : nodesMgr.getPigeonTypes().entrySet()) {
                List<HashRange> list = entry.getValue();
                if (list.size() != hosts.length) {
                    System.out.println("Range count != new hosts.length; count = " + list.size());
                    return;
                }
                List<HashRange> expandList = new ArrayList<HashRange>();
                for (int i = 0; i < list.size(); i++) {
                    HashRange hr = list.get(i);
                    List<PigeonNode> listNode = hr.getMembers();
                    HashRange ehr = new HashRange(hr.getRange());
                    HashSet<String> exist = new HashSet<String>();
                    for (PigeonNode pn : hr.getMembers()) {
                        PigeonNode npn = (PigeonNode) BeanUtils.cloneBean(pn);
                        String host = "";
                        for (int j = 0; j < roles.length; j++) {
                            if (npn.getRole() == roles[j].charAt(0)) {
                                host = fetchString(hosts, i + j);
                                break;
                            }
                        }
                        if (host.isEmpty()) {
                            System.out.println("bad role = " + npn.getRole());
                            return;
                        }
                        if (exist.contains(String.valueOf(npn.getRole()))) {
                            System.out.println("warning : reduplicate role = " + String.valueOf(npn.getRole()));
                            continue;
                        }
                        npn.setHost(host);
                        ehr.addPigeonNode(npn);
                        makePigeonNodes(npn, new String[]{"pigeon30master", "pigeon30sync", "pigeon30backup"}, args[args.length - 2], args[args.length - 1]);
                        exist.add(String.valueOf(npn.getRole()));
                    }
                    String[] ranges = expandRange(hr.getRange());
                    hr.setRange(ranges[0]);
                    ehr.setRange(ranges[1]);
                    expandList.add(hr);
                    expandList.add(ehr);
                }
                mapRanges.put(entry.getKey(), expandList);
            }
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("version", version);
            JSONArray pigeon_nodes = new JSONArray();
            for (String type : mapRanges.keySet()) {
                JSONObject jsonType = new JSONObject();
                jsonType.put("type", type);
                JSONArray segments = new JSONArray();
                List<HashRange> lhr = mapRanges.get(type);
                for (HashRange hr : lhr) {
                    JSONObject jsonRange = new JSONObject();
                    jsonRange.put("range", hr.getRange());
                    JSONArray members = new JSONArray();
                    for (PigeonNode pn : hr.getMembers()) {
                        JSONObject jsonNode = new JSONObject();
                        jsonNode.put("host", pn.getHost());
                        jsonNode.put("name", pn.getName());
                        jsonNode.put("port", pn.getPort());
                        jsonNode.put("role", String.valueOf(pn.getRole()));
                        members.put(jsonNode);
                    }
                    jsonRange.put("members", members);
                    segments.put(jsonRange);
                }
                jsonType.put("segments", segments);
                pigeon_nodes.put(jsonType);
            }
            jsonObject.put("pigeon_nodes", pigeon_nodes);
            String pigeonnodes = jsonObject.toString(8);
            saveConfig(version + "_pigeonnodes.conf", pigeonnodes);
        }
    }

}

