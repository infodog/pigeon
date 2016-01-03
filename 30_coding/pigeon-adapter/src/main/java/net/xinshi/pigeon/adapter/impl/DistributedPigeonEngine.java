package net.xinshi.pigeon.adapter.impl;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.atom.IIntegerAtom;
import net.xinshi.pigeon.client.distributedclient.atomclient.DistributedAtom;
import net.xinshi.pigeon.client.distributedclient.flexobjectclient.DistributedFlexObjectFactory;
import net.xinshi.pigeon.client.distributedclient.idclient.DistributedIdGenerator;
import net.xinshi.pigeon.client.distributedclient.listclient.DistributedListFactory;
import net.xinshi.pigeon.client.distributedclient.lockclient.DistributedNettyLock;
import net.xinshi.pigeon.distributed.Constants;
import net.xinshi.pigeon.distributed.client.ClientNode;
import net.xinshi.pigeon.distributed.client.NodesDispatcher;
import net.xinshi.pigeon.distributed.lic.CheckLicense;
import net.xinshi.pigeon.flexobject.IFlexObjectFactory;
import net.xinshi.pigeon.idgenerator.IIDGenerator;
import net.xinshi.pigeon.list.IListFactory;
import net.xinshi.pigeon.netty.common.PigeonFuture;
import net.xinshi.pigeon.resourcelock.IResourceLock;
import net.xinshi.pigeon.util.CommonTools;
import org.apache.commons.lang.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-7
 * Time: 上午10:49
 * To change this template use File | Settings | File Templates.
 */

public class DistributedPigeonEngine implements IPigeonStoreEngine {

    private NodesDispatcher nodesDispatcher = null;

    DistributedFlexObjectFactory flexobjectFactory;
    DistributedListFactory listFactory;
    DistributedAtom atom;
    DistributedIdGenerator idGenerator;
    DistributedNettyLock lock;

    Logger logger = Logger.getLogger(DistributedPigeonEngine.class.getName());

    public DistributedPigeonEngine(String config) throws Exception {
        nodesDispatcher = new NodesDispatcher(config);
        init();
    }

    public DistributedPigeonEngine(String config, boolean asResource) throws Exception {
        nodesDispatcher = new NodesDispatcher(config, asResource);
        init();
    }

    public DistributedPigeonEngine(String host, String port) {
        nodesDispatcher = new NodesDispatcher(host, Integer.valueOf(port));
    }

    @Override
    public IFlexObjectFactory getFlexObjectFactory() {
        return flexobjectFactory;
    }

    @Override
    public IListFactory getListFactory() {
        return listFactory;
    }

    @Override
    public IIntegerAtom getAtom() {
        return atom;
    }

    @Override
    public IIDGenerator getIdGenerator() {
        return idGenerator;
    }

    public IResourceLock getLock() {
        return lock;
    }

    class CheckWorker implements Runnable {
        public void run() {
            Thread.currentThread().setName("CheckWorker_run");
            while (true) {
                try {
                    nodesDispatcher.licOK = CheckLicense.check();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    Thread.sleep(1000 * 600);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        public boolean isOK() {
            System.out.println("pigeon version = " + net.xinshi.pigeon.distributed.Constants.version);
            nodesDispatcher.licOK = false;
            try {
                nodesDispatcher.licOK = CheckLicense.check();
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (nodesDispatcher.licOK) {
                new Thread(this).start();
            }
            System.out.println("nodesDispatcher.licOK = " + nodesDispatcher.licOK);
            return nodesDispatcher.licOK;
        }
    }

    void init() throws Exception {

        CheckWorker checkWorker = new CheckWorker();
        if (!checkWorker.isOK()) {
            throw new Exception("check pigeon.lic failed !!!");
        }

        nodesDispatcher.init();

        flexobjectFactory = new DistributedFlexObjectFactory(nodesDispatcher);
        flexobjectFactory.init();

        listFactory = new DistributedListFactory(nodesDispatcher);
        listFactory.init();

        atom = new DistributedAtom(nodesDispatcher);
        atom.init();

        lock = new DistributedNettyLock(nodesDispatcher);
        lock.init();

        long idNumPerRound = 10000L;
        idGenerator = new DistributedIdGenerator(nodesDispatcher, idNumPerRound);
        idGenerator.init();

    }

    @Override
    public void stop() throws InterruptedException {
        try {
            if (lock != null) {
                lock.stop();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Map<String, List<String>> backup(String key) throws Exception {
        Map<String, List<String>> map = new HashMap<String, List<String>>();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "backup");
        if (key != null) {
            CommonTools.writeString(out, key);
        }
        List<PigeonFuture> listAtom = nodesDispatcher.CommitAsync("atom", out);
        List<PigeonFuture> listId = nodesDispatcher.CommitAsync("idserver", out);
        List<PigeonFuture> listObj = nodesDispatcher.CommitAsync("flexobject", out);
        List<PigeonFuture> listList = nodesDispatcher.CommitAsync("list", out);
        List<PigeonFuture> listAll = new ArrayList<PigeonFuture>();
        listAll.addAll(listAtom);
        listAll.addAll(listId);
        listAll.addAll(listObj);
        listAll.addAll(listList);

        try {
            for (PigeonFuture pf : listAll) {
                if (pf == null) {
                    throw new Exception("DistributedPigeonEngine.backup(), pf == null");
                }
                if (!pf.waitme(1000 * 600)) {
                    throw new Exception("DistributedPigeonEngine.backup(), pf wait timeout");
                }
                InputStream in = new ByteArrayInputStream(pf.getData(), Constants.PACKET_PREFIX_LENGTH, pf.getData().length - Constants.PACKET_PREFIX_LENGTH);
                String state = CommonTools.readString(in);
                if (StringUtils.equals("OK", state)) {
                    String path = CommonTools.readString(in);
                    path = "http://" + pf.getHost() + path;
                    if (listAtom.contains(pf)) {
                        List<String> list = map.get("atom");
                        if (list == null) {
                            list = new ArrayList<String>();
                            map.put("atom", list);
                        }
                        list.add(path);
                    } else if (listId.contains(pf)) {
                        List<String> list = map.get("idserver");
                        if (list == null) {
                            list = new ArrayList<String>();
                            map.put("idserver", list);
                        }
                        list.add(path);
                    } else if (listObj.contains(pf)) {
                        List<String> list = map.get("flexobject");
                        if (list == null) {
                            list = new ArrayList<String>();
                            map.put("flexobject", list);
                        }
                        list.add(path);
                    } else if (listList.contains(pf)) {
                        List<String> list = map.get("list");
                        if (list == null) {
                            list = new ArrayList<String>();
                            map.put("list", list);
                        }
                        list.add(path);
                    }
                } else {
                    throw new Exception("DistributedPigeonEngine.backup(), pf return code != OK");
                }
            }
        } catch (Exception e) {
            for (PigeonFuture pf : listAll) {
                pf.waitme(10);
            }
            throw e;
        }

        return map;
    }

    public List<ClientNode> get_kv_m_nodes() {
        return nodesDispatcher.get_kv_m_nodes();
    }

}

