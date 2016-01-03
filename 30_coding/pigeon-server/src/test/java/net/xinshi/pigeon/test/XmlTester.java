package net.xinshi.pigeon.test;

import org.dom4j.*;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: WPF
 * Date: 13-11-18
 * Time: 上午10:08
 * To change this template use File | Settings | File Templates.
 */
public class XmlTester {

    static Document document1;
    static Element root1;
    static int no = 0;

    static void add_xml(String c, String p, String d, String m, String q) throws Exception {
        Element element1 = root1.addElement("crontabentry");
        element1.addAttribute("id", "" + ++no);
        element1.addElement("queue").addText(q);
        element1.addElement("class").addText(c);
        element1.addElement("method").addText(m);
        element1.addElement("parameters").addText(p);
        element1.addElement("description").addText(d);
    }

    static void add_xml(Element elt, String c, String p) throws Exception {
        elt.attribute("id").setValue("" + ++no);
        elt.element("class").setText(c);
        elt.element("parameters").setText(p);
        root1.add((Element) elt.clone());
    }

    public static void main(String[] args) throws Exception {

        root1 = DocumentHelper.createElement("crontab");
        document1 = DocumentHelper.createDocument(root1);

        /*
        String x = "....".substring(0, 0);

        BufferedReader br = new BufferedReader(new FileReader(("d:/00")));
        String line;
        HashMap<String, String> map = new HashMap<String, String>();

        while ((line = br.readLine()) != null) {
            line = line.trim();
            if (line.length() == 0) {
                continue;
            }
            String[] elts = line.split(" ");
            map.put(elts[0], elts[1]);
        }*/

        Document document = new SAXReader().read("d:/crontab.xml").getDocument();

        int i = 1;

        for (Element crontabentry : (List<Element>) document.getRootElement().elements()) {

            String cs = crontabentry.element("class").getText();
            // System.out.println(i + "." + cs);
            //     System.out.println(i + "." + crontabentry.element("method").getText());
            //     System.out.println(i + "." + crontabentry.element("parameters").getText());
            //    System.out.println(i + "." + crontabentry.element("description").getText());

            /*String queue = map.get(crontabentry.element("class").getText());
            if (queue != null) {
                crontabentry.addEntity("<queue>", "<queue>" + queue + "</queue>");
            }*/

            if (cs.indexOf("CronAgent") < 0) {
                //  continue;
            }

            System.out.println(cs);

            String p = "";
            if (crontabentry.element("parameters") != null) {
                p = crontabentry.element("parameters").getText();
            }

          /*  String[] elts = p.split(" ");
            cs = elts[0];
            if (elts.length > 1) {
                String t = "";
                for (int j = 1; j < elts.length; j++) {
                    t += elts[j];
                    t += " ";
                }
                p = t.trim();
            } else {
                p = "";
            }*/


            String m = "run";
            if (crontabentry.element("method") != null) {
                m = crontabentry.element("method").getText();
            }
            String d = "";
            if (crontabentry.element("description") != null) {
                d = crontabentry.element("description").getText();
            }

           /* String queue = null;
            for (Map.Entry<String, String> entry : map.entrySet()) {
                if (entry.getValue().equals(cs)) {
                    queue = entry.getKey();
                    map.remove(queue);
                    break;
                }
            }

            if (queue != null) {
                //add_xml(cs, p, d, m, queue);
                //map.remove(cs);
                continue;
            }*/

            add_xml(crontabentry, "net.xinshi.saasadmin.cron.CronAgent", (cs + " " + p).trim());


            System.out.println(p);

            i++;
        }

        /*for ( String k : map.keySet()) {
            add_xml(k, "", "", "run", map.get(k));
        }*/

        XMLWriter xmlWriter = new XMLWriter();
        System.out.println("");
        System.out.println("");
        xmlWriter.write(document1);
        System.out.println("");
        System.out.println("");

    }
}
