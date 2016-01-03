package net.xinshi.pigeon.test;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.util.*;

public class Json2Excel {

    /*
    public static Map<String, String> partWord(File file) {
        Map<String, String> map2 = new HashMap<String, String>();
        InputStream in = null;
        try {
            in = new FileInputStream(file);//得到文件流，File    file    =    new    File("D:\\hah.txt");
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String str;
            while ((str = br.readLine()) != null) {
                str = StringUtils.substring(str, str.indexOf('(') + 1, str.lastIndexOf(')'));
                Gson gson = new Gson();
                Map<String, Object> map = gson.fromJson(str, HashMap.class);//解析语句，解析的结果是{s=[逛街],    q=衣服图,    p=false}
                List<String> newList = new ArrayList<String>();

                for (String t : (List<String>) map.get("s")) {
                    newList.add(StringUtils.remove(t, (String) map.get("q")));//从语句中删除某一词
                }
                String result = newList.toString();
                if (result != null) {
                    map2.put((String) map.get("q"), result);
                } else {
                    map2.put((String) map.get("q"), null);
                }
            }
            return map2;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map2;
    }



    public static void writeExcel(String path, Map<String, String> content,
                                  String sheetName, int sheetNum, int startRow) {

        try {

            //    打开文件
            WritableWorkbook book = Workbook.createWorkbook(new File(path));

//    生成名为“第一页”的工作表，参数0表示这是第一页
            WritableSheet sheet = book.createSheet(sheetName, sheetNum);

//    在Label对象的构造子中指名单元格位置是第一列第一行(0,0)
//    以及单元格内容为test
            int start = startRow;
            for (String key : content.keySet()) {

                Label label1 = new Label(0, start, key);
                Label label2 = new Label(1, start, content.get(key));

//    将定义好的单元格添加到工作表中
                sheet.addCell(label1);
                sheet.addCell(label2);
                start++;
            }

            //    写入数据并关闭文件
            book.write();
            book.close();
        } catch (Exception e) {
            System.out.println(e);

        }

    }
    */

    public static class cmp implements Comparator<String> {
        public int compare(String o1, String o2) {
            int i1 = o1.lastIndexOf('\\');
            int i2 = o2.lastIndexOf('\\');
            return Integer.valueOf(o1.substring(i1 + 1)) - Integer.valueOf(o2.substring(i2 + 1));
        }
    }

    public static void main(String argv[]) throws Exception {
        try {

            int nr = 0;

            Map<String, Integer> mapLable = new HashMap<String, Integer>();
            Map<String, Integer> mapUID = new HashMap<String, Integer>();

            String outputFile = "D:\\kuaican5.xls";
            String htmlFile = "d:\\kuaican5.html";

            FileOutputStream fOut = new FileOutputStream(outputFile);
            FileOutputStream fOut2 = new FileOutputStream(htmlFile);

            HSSFWorkbook workbook = new HSSFWorkbook();//创建新的Excel工作薄

            HSSFSheet sheet = workbook.createSheet("快餐");//新建一名为“效益指标”的工作表

            File f = new File("D:\\map4.tar\\map4");
            File[] fs = f.listFiles();

            fOut2.write(("<table border=\"1\" width=\"80%\" bgcolor=\"#E8E8E8\" cellpadding=\"2\" bordercolor=\"#0000FF\"\n" + "bordercolorlight=\"#7D7DFF\" bordercolordark=\"#0000A0\">\n").getBytes());

            for (File tfs : fs) {
                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(tfs)));
                String str;
                while ((str = br.readLine()) != null) {
                    JSONObject jso = new JSONObject(str);
                    JSONArray jsa;
                    try {
                        jsa = jso.getJSONArray("content");
                    } catch (Exception e) {
                        System.out.println(str);
                        continue;
                    }
                    for (int i = 0; i < jsa.length(); i++) {
                        JSONObject jso2 = (JSONObject) jsa.get(i);
                        int j = 0;
                        for (Object key : jso2.getObjectMap().keySet()) {
                            Object val = (Object) jso2.get((String) key);
                            if (val instanceof String) {
                                Integer vi = mapLable.get((String) key);
                                if (vi == null) {
                                    vi = 0;
                                }
                                vi += ((String) val).trim().length();
                                mapLable.put((String) key, vi);
                            }
                        }
                    }
                }
            }

            for (String key : mapLable.keySet()) {
                if (mapLable.get(key) < 1) {
                    mapLable.remove(key);
                }
            }

            mapLable.remove("storage_src");
            mapLable.remove("uid");
            mapLable.remove("geo");
            mapLable.remove("cp");

            int ik = 0;

            for (String key : mapLable.keySet()) {
                mapLable.put(key, ik++);
            }

            List<String> files = new ArrayList<String>();


            for (File tfs : fs) {
                files.add(tfs.getAbsolutePath());
            }

            Collections.sort(files, new cmp());

            for (String ttf : files) {

                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(ttf)));


                String str;
                while ((str = br.readLine()) != null) {
                    JSONObject jso = new JSONObject(str);
                    JSONArray jsa;
                    try {
                        jsa = jso.getJSONArray("content");
                    } catch (Exception e) {
                        System.out.println(str);
                        continue;
                    }

                    for (int i = 0; i < jsa.length(); i++) {
                        JSONObject jso2 = (JSONObject) jsa.get(i);
                        System.out.println(jso2);

                        HSSFRow row = sheet.createRow(nr++);

                        String html = "<tr>";

                        html += "<td>" + nr + "</td>";

                        String td[] = new String[100];

                        for (Object key : jso2.getObjectMap().keySet()) {
                            Object val = (Object) jso2.get((String) key);
                            String ek = (String) key;
                            if (ek.equals("uid") && mapUID.get((String) val) != null) {
                                sheet.removeRow(row);
                                --nr;
                                break;
                            }
                            if (val instanceof String) {
                                mapUID.put((String) val, 1);
                                Integer j = mapLable.get((String) key);
                                if (j == null) {
                                    continue;
                                }
                                HSSFCell cell = row.createCell(j, HSSFCell.CELL_TYPE_STRING);
                                cell.setCellValue((String) val);
                                // html += "<td>" + val + "</td>";
                                // System.out.println(val);
                                td[j] = new String((String) val);
                            }
                        }

                        for (int ti = 0; ti < mapLable.size(); ti++) {
                            if (td[ti] != null) {
                                html += "<td>" + td[ti] + "</td>";
                            } else {
                                html += "<td></td>";
                            }
                        }

                        html += "</tr>\r\n";
                        fOut2.write(html.getBytes());

                    }

                    fOut2.flush();

                }
            }

            workbook.write(fOut);
            fOut.flush();
            fOut.close();

            fOut2.write("</table>".getBytes());

            fOut2.close();

            System.out.println("file size = " + fs.length);

            for (String key : mapLable.keySet()) {
                System.out.println(key + "    ");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
