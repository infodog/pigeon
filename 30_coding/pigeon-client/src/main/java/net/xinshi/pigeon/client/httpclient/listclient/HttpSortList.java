package net.xinshi.pigeon.client.httpclient.listclient;

import net.xinshi.pigeon.list.ISortList;
import net.xinshi.pigeon.list.SortListObject;
import net.xinshi.pigeon.util.CommonTools;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Vector;

/**
 * Created by IntelliJ IDEA.
 * User: zhengxiangyang
 * Date: 11-11-1
 * Time: 上午10:54
 * To change this template use File | Settings | File Templates.
 */
public class HttpSortList implements ISortList {
    String listId;
    String url;
    HttpClient httpClient;

    public String getListId() {
        return listId;
    }

    public void setListId(String listId) {
        this.listId = listId;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public HttpClient getHttpClient() {
        return httpClient;
    }

    public void setHttpClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }


    List<SortListObject> getListObjects(String content) throws Exception {
        String[] sobjs = content.split(";");
        Vector result = new Vector();
        for (String sobj : sobjs) {
            if (StringUtils.isBlank(sobj)) {
                break;
            }

            String[] oneSobj = sobj.split(",");
            if (oneSobj.length != 2) {
                break;
            }
            SortListObject retObj = new SortListObject(oneSobj[0], oneSobj[1]);
            result.add(retObj);
        }
        return result;
    }

    public List<SortListObject> getRange(int beginIndex, int number) throws Exception {
        HttpPost httpPost = new HttpPost(url);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "getRange");
        CommonTools.writeString(out, listId);
        CommonTools.writeString(out, "" + beginIndex);
        CommonTools.writeString(out, "" + number);

        httpPost.setEntity(new ByteArrayEntity(out.toByteArray()));

        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();
        try {
            if (entity != null) {
                InputStream in = entity.getContent();
                String state = CommonTools.readString(in);
                if (StringUtils.equals(state, "ok")) {
                    String content = CommonTools.readString(in);
                    if (content.startsWith("error")) {
                        throw new Exception("something wrong with the server," + content);
                    }
                    return getListObjects(content);
                } else {
                    throw new Exception("something wrong with the server," + state);
                }
            } else {
                return null;
            }
        } finally {
            if (entity != null) {
                entity.consumeContent();
            }
        }
    }

    public boolean delete(SortListObject sortObj) throws Exception {
        HttpPost httpPost = new HttpPost(url);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "delete");
        CommonTools.writeString(out, listId);
        CommonTools.writeString(out, sortObj.getKey());
        CommonTools.writeString(out, sortObj.getObjid());
        httpPost.setEntity(new ByteArrayEntity(out.toByteArray()));

        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();
        try {
            if (entity != null) {
                InputStream in = entity.getContent();
                String content = CommonTools.readString(in);
                if (content != null && content.trim().equals("ok")) {
                    return true;
                }
            }
        } finally {
            if (entity != null) {
                entity.consumeContent();
            }
        }
        return false;
    }

    public boolean add(SortListObject sortObj) throws Exception {
        HttpPost httpPost = new HttpPost(url);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "add");
        CommonTools.writeString(out, listId);
        CommonTools.writeString(out, sortObj.getKey());
        CommonTools.writeString(out, sortObj.getObjid());
        httpPost.setEntity(new ByteArrayEntity(out.toByteArray()));
        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();
        try {
            if (entity != null) {
                InputStream in = entity.getContent();
                String content = CommonTools.readString(in);
                if (content != null && content.trim().equals("ok")) {
                    return true;
                } else if (StringUtils.equals("ok dup", content)) {
                    return false;
                } else {
                    throw new Exception(content);
                }
            }
        } finally {
            entity.consumeContent();
        }
        return false;
    }

    public boolean add(List<SortListObject> listSortObj) throws Exception {
        throw new Exception("not implement list add(list<>) method!!!");
    }

    public boolean reorder(SortListObject oldObj, SortListObject newObj) throws Exception {
        HttpPost httpPost = new HttpPost(url);


        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "reorder");
        CommonTools.writeString(out, listId);
        CommonTools.writeString(out, oldObj.getKey());
        CommonTools.writeString(out, oldObj.getObjid());
        CommonTools.writeString(out, newObj.getKey());
        CommonTools.writeString(out, newObj.getObjid());
        httpPost.setEntity(new ByteArrayEntity(out.toByteArray()));

        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();
        try {
            if (entity != null) {
                InputStream in = entity.getContent();
                String state = CommonTools.readString(in);
                if (StringUtils.equals(state, "ok")) {
                    return true;
                } else {
                    return false;
                }
            }
        } finally {
            if (entity != null) {
                entity.consumeContent();
            }
        }
        return false;
    }

    public boolean isExists(String key, String objid) throws Exception {
        HttpPost httpPost = new HttpPost(url);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "isExists");
        CommonTools.writeString(out, listId);
        CommonTools.writeString(out, key);
        CommonTools.writeString(out, objid);
        httpPost.setEntity(new ByteArrayEntity(out.toByteArray()));

        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();
        try {
            if (entity != null) {
                InputStream in = entity.getContent();
                String state = CommonTools.readString(in);
                if (StringUtils.equals(state, "ok")) {
                    String content = CommonTools.readString(in);
                    if (content != null && content.trim().equals("y")) {
                        return true;
                    } else if (content != null && content.trim().equals("n")) {
                        return false;
                    }
                } else {
                    throw new Exception("Server error.state=" + state);
                }
            }
        } finally {
            if (entity != null) {
                entity.consumeContent();
            }
        }
        throw new Exception("server return wrong format data.");
    }

    public long getLessOrEqualPos(SortListObject obj) throws Exception {
        HttpPost httpPost = new HttpPost(url);


        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "getLessOrEqualPos");
        CommonTools.writeString(out, listId);
        CommonTools.writeString(out, obj.getKey());
        CommonTools.writeString(out, obj.getObjid());
        httpPost.setEntity(new ByteArrayEntity(out.toByteArray()));

        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();
        try {
            if (entity != null) {
                InputStream in = entity.getContent();
                String state = CommonTools.readString(in);
                if (StringUtils.equals(state, "ok")) {
                    String content = CommonTools.readString(in);
                    if (!StringUtils.isNumeric(content)) {
                        throw new Exception("server return wrong format data.");
                    } else {
                        return Long.parseLong(content);
                    }
                } else {
                    throw new Exception("server return wrong format data.state=" + state);
                }
            }
        } finally {
            if (entity != null) {
                entity.consumeContent();
            }
        }
        throw new Exception("server return wrong format data.");
    }

    public SortListObject getSortListObject(String key) throws Exception {
        HttpPost httpPost = new HttpPost(url);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "getSortListObject");
        CommonTools.writeString(out, listId);
        CommonTools.writeString(out, key);
        httpPost.setEntity(new ByteArrayEntity(out.toByteArray()));

        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();
        try {
            if (entity != null) {
                InputStream in = entity.getContent();
                String state = CommonTools.readString(in);
                if (StringUtils.equals(state, "ok")) {
                    String content = CommonTools.readString(in);
                    if (StringUtils.equals(content, "null")) {
                        return null;
                    }
                    JSONObject jobj = new JSONObject(content);
                    return new SortListObject(jobj.getString("key"), jobj.getString("objid"));
                } else {
                    throw new Exception("server error.");
                }
            }
            throw new Exception("server return wrong format data.");
        } finally {
            if (entity != null) {
                entity.consumeContent();
            }
        }
    }

    public List<SortListObject> getHigherOrEqual(SortListObject obj, int num) throws Exception {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public long getSize() throws Exception {
        HttpPost httpPost = new HttpPost(url);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "getSize");
        CommonTools.writeString(out, listId);
        httpPost.setEntity(new ByteArrayEntity(out.toByteArray()));

        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();
        try {
            if (entity != null) {
                InputStream in = entity.getContent();
                String state = CommonTools.readString(in);
                if (StringUtils.equals(state, "ok")) {
                    String content = CommonTools.readString(in);
                    if (!StringUtils.isNumeric(content)) {
                        throw new Exception("server return wrong format data.");
                    } else {
                        return Long.parseLong(content);
                    }
                } else {
                    throw new Exception("server state error state =" + state);
                }
            }
        } finally {
            if (entity != null) {
                entity.consumeContent();
            }
        }
        throw new Exception("server return wrong format data.");
    }


}
