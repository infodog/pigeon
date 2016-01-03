package net.xinshi.pigeon.distributed.duplicate;

import net.xinshi.pigeon.distributed.bean.HashRange;
import net.xinshi.pigeon.distributed.bean.PigeonNode;
import net.xinshi.pigeon.distributed.bean.ServerConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-19
 * Time: 上午9:50
 * To change this template use File | Settings | File Templates.
 */

public class DuplicateServer {

    char role = '0';
    ServerConfig sc;
    List<PigeonNode> listMasterPNs;
    List<PigeonNode> listSyncPNs;
    List<PigeonNode> listBackupPNs;

    public DuplicateServer(ServerConfig sc) {
        this.sc = sc;
        listMasterPNs = new ArrayList<PigeonNode>();
        listSyncPNs = new ArrayList<PigeonNode>();
        listBackupPNs = new ArrayList<PigeonNode>();
    }

    public char getRole() {
        return role;
    }

    public void setRole(char role) {
        this.role = role;
    }

    public ServerConfig getSc() {
        return sc;
    }

    public void setSc(ServerConfig sc) {
        this.sc = sc;
    }

    public List<PigeonNode> getListMasterPNs() {
        return listMasterPNs;
    }

    public void setListMasterPNs(List<PigeonNode> listMasterPNs) {
        this.listMasterPNs = listMasterPNs;
    }

    public List<PigeonNode> getListSyncPNs() {
        return listSyncPNs;
    }

    public void setListSyncPNs(List<PigeonNode> listSyncPNs) {
        this.listSyncPNs = listSyncPNs;
    }

    public List<PigeonNode> getListBackupPNs() {
        return listBackupPNs;
    }

    public void setListBackupPNs(List<PigeonNode> listBackupPNs) {
        this.listBackupPNs = listBackupPNs;
    }

    private boolean exists(List<PigeonNode> pns, PigeonNode pn) {
        for (PigeonNode p : pns) {
            if (p == pn || p.getHost().compareToIgnoreCase(pn.getHost()) == 0 && p.getPort() == pn.getPort() && p.getName().equals(pn.getName())) {
                return true;
            }
        }
        return false;
    }

    private String nodesString(List<PigeonNode> pns) {
        String info = "";
        for (PigeonNode pn : pns) {
            info += " " + pn.getHost() + "_" + pn.getPort() + "_" + pn.getName();
        }
        return info;
    }

    public String getNodesString() {
        String info = "";
        if (listMasterPNs.size() > 0) {
            info += "Master : " + nodesString(listMasterPNs);
        }
        if (listSyncPNs.size() > 0) {
            info += " Sync : " + nodesString(listSyncPNs);
        }
        if (listBackupPNs.size() > 0) {
            info += " Backup : " + nodesString(listBackupPNs);
        }
        return info;
    }

    public void printNodes() {
        System.out.println(sc.getNodeName() + " [" + role + "] Master : " + nodesString(listMasterPNs));
        System.out.println(sc.getNodeName() + " [" + role + "] Sync : " + nodesString(listSyncPNs));
        System.out.println(sc.getNodeName() + " [" + role + "] Backup : " + nodesString(listBackupPNs));
    }

    public void init(HashMap<String, List<HashRange>> PigeonTypes) throws Exception {
        String[] parts = sc.getNodeName().split("_");
        if (parts == null || parts.length != 3) {
            throw new Exception("ServerConfig nodeName error must (host_port_name) ...... ");
        }
        List<HashRange> listHR = PigeonTypes.get(sc.getType());
        if (listHR == null) {
            setRole('M');
            return;
        }
        for (HashRange hr : listHR) {
            PigeonNode me = null;
            List<PigeonNode> listPN = new ArrayList<PigeonNode>();
            for (PigeonNode pn : hr.getMembers()) {
                if (pn.getHost().compareToIgnoreCase(parts[0]) == 0 && pn.getPort() == Integer.valueOf(parts[1]) && pn.getName().equals(parts[2])) {
                    me = pn;
                    if (role == '0') {
                        role = me.getRole();
                    } else if (role != me.getRole()) {
                        throw new Exception("the same node has two roles ... not support ... ");
                    }
                } else {
                    listPN.add(pn);
                }
            }
            if (me != null) {
                for (PigeonNode pn : listPN) {
                    if (("/" + pn.getName()).compareTo(sc.getInstanceName()) != 0) {
                        continue;
                    }
                    if (pn.getRole() == 'M') {
                        if (!exists(listMasterPNs, pn)) {
                            listMasterPNs.add(pn);
                        }
                    } else if (pn.getRole() == 'S') {
                        if (!exists(listSyncPNs, pn)) {
                            listSyncPNs.add(pn);
                        }
                    } else if (pn.getRole() == 'B') {
                        if (!exists(listBackupPNs, pn)) {
                            listBackupPNs.add(pn);
                        }
                    }
                }
                Collections.sort(listMasterPNs);
                Collections.sort(listSyncPNs);
                Collections.sort(listBackupPNs);
            }
        }
        if (role == '0') {
            setRole('M');
        }
        if (listMasterPNs.size() > 1) {
            throw new Exception(sc.getNodeName() + " listMasterPNs.size() > 1 not support");
        }
        if (listSyncPNs.size() > 1) {
            throw new Exception(sc.getNodeName() + " listSyncPNs.size() > 1 not support");
        }
    }

}

