package net.xinshi.pigeon.clienttools;

import net.xinshi.pigeon.adapter.IPigeonStoreEngine;
import net.xinshi.pigeon.list.ISortList;
import net.xinshi.pigeon.list.SortListObject;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.Vector;

/**
 * Created by IntelliJ IDEA.
 * User: mac
 * Date: 11-11-29
 * Time: 下午3:39
 * To change this template use File | Settings | File Templates.
 */
public class PigeonScriptTools {
    static public Vector executeScript(IPigeonStoreEngine pigeonStore,String script, List<String> errors) throws Exception {
        /*
        [
            { 
                action:'putobject',
                id:'dddddd',
                content:{adadfad.ddd}                               ``````````````````````````````````````

            },
            {
                action:'putatom',
                id:'ddd',
                value:1111
            },
            {
                action:'putlist',
                id:'xxx',
                value:[{key:'dddd',objid:'kkkkkk'}]
            },{
                action:getObject
                id:"xxx"
            },{
                action:getList
                id:"xxx"
            },{
                action:getAtom
                id:"xxx"
              }



        ]
        */

        String errmsg = null;
        Vector<String> msgs = new Vector();
        try {
            JSONArray jscript = new JSONArray(script);
            for (int i = 0; i < jscript.length(); i++) {
                JSONObject jcmd = jscript.getJSONObject(i);
                String action = jcmd.getString("action");
                if (action.equals("putobject")) {
                    String id = jcmd.getString("id");
                    Object o = jcmd.opt("content");
                    if (o == null) {
                        pigeonStore.getFlexObjectFactory().saveContent(id, "");
                    } else if (o instanceof JSONObject) {
                        pigeonStore.getFlexObjectFactory().saveContent(id, o.toString());
                        msgs.add("成功保存了对象 " + id);
                    }

                } else if (action.equals("putatom")) {
                    String id = jcmd.getString("id");
                    int value = jcmd.getInt("value");
                    pigeonStore.getAtom().createAndSet(id, value);
                    msgs.add("成功设置了atom " + id);
                } else if (action.equals("putlist")) {
                    String id = jcmd.getString("id");
                    JSONArray jlist = jcmd.getJSONArray("value");
                    ISortList list = pigeonStore.getListFactory().getList(id, true);
                    for (int j = 0; j < jlist.length(); j++) {
                        try {
                            JSONObject jobj = jlist.getJSONObject(j);
                            SortListObject sobj = new SortListObject();
                            sobj.setKey(jobj.getString("key"));
                            sobj.setObjid(jobj.getString("objid"));
                            list.add(sobj);
                        } catch (Exception ex) {
                            //ex.printStackTrace();
                            errors.add(ex.getMessage());
                        }
                    }
                    msgs.add("成功添加了list " + id);
                } else if (action.equals("clearlist")) {
                    String id = jcmd.getString("id");
                    ISortList list = pigeonStore.getListFactory().getList(id, true);
                    List<SortListObject> sobjList = list.getRange(0, (int) list.getSize());
                    for (SortListObject sobj : sobjList) {
                        list.delete(sobj);
                    }
                    msgs.add("成功清空了list " + id);
                } else if (action.equals("getObject")) {
                    String id = jcmd.getString("id");
                    String content = pigeonStore.getFlexObjectFactory().getContent(id);
                    msgs.add("getObject " + id + ":" + content);
                } else if (action.equals("getList")) {
                    String id = jcmd.getString("id");
                    ISortList list = pigeonStore.getListFactory().getList(id, false);
                    if (list == null) {
                        msgs.add("list " + id + " is null");
                    } else {
                        List<SortListObject> slist = list.getRange(0, (int) list.getSize());
                        StringBuilder sb = new StringBuilder();
                        for (SortListObject sobj : slist) {
                            sb.append(sobj.getObjid()).append(",").append(sobj.getKey()).append("\n");
                        }
                        msgs.add("list " + id + ":\n" + sb.toString());
                    }
                } else if (action.equals("getAtom")) {
                    String id = jcmd.getString("id");
                    Long atom = pigeonStore.getAtom().get(id);
                    if (atom == null) {
                        msgs.add("atom " + id + " is null");
                    } else {
                        msgs.add("atom " + id + " = " + atom);
                    }
                } else if (action.equals("deleteList")) {
                    String id = jcmd.getString("id");
                    String key = jcmd.getString("key");
                    String objId = jcmd.getString("objId");
                    ISortList list = pigeonStore.getListFactory().getList(id, true);

                    SortListObject sobj = new SortListObject();
                    sobj.setKey(key);
                    sobj.setObjid(objId);
                    list.delete(sobj);

                    msgs.add("成功删除了对象： " + id);
                }
            }
        } catch (Exception e) {
            //errmsg = e.getMessage();
            //e.printStackTrace();
            //throw e;
            errors.add(e.getMessage());
        }
        return msgs;
    }

    public static Vector execFile(IPigeonStoreEngine pigeonStore,File f) throws Exception {
        FileInputStream fin = new FileInputStream(f);

        byte[] buf = new byte[(int) f.length()];
        fin.read(buf);
        String script = new String(buf, "utf-8");
        fin.close();
        Vector<String> errors = new Vector<String>();
        Vector<String> msgs = executeScript(pigeonStore,script, errors);
        if (errors.size() > 0) {
            System.out.println(f.getAbsoluteFile() + "执行出错：");
            for (String error : errors) {
                System.out.println(error);
            }
        }
        return msgs;
    }

    public static Vector execAll(IPigeonStoreEngine pigeonStore,java.io.File f) {
        Vector result = new Vector();
        if (f.isDirectory()) {
            File[] files = f.listFiles();
            for (int i = 0; i < files.length; i++) {
                result.addAll(execAll(pigeonStore,files[i]));
            }
        } else if (f.isFile()) {
            if (f.getName().endsWith(".js")) {
                try {
                    result.addAll(execFile(pigeonStore,f));
                } catch (Exception e) {
                    result.insertElementAt("保存失败" + f.getAbsolutePath() + " " + e.getMessage() + "<br>\r\n", 0);
                    System.out.println("执行失败" + f.getAbsoluteFile());
                    //e.printStackTrace();
                }
            }
        }
        return result;

    }
}
