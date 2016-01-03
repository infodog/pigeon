<%@ page contentType="text/html;charset=UTF-8" %>
<%@ page import="net.xinshi.pigeon.adapter.impl.NormalPigeonEngine" %>
<%@ page import="net.xinshi.pigeon.adapter.StaticPigeonEngine" %>
<%@ page import="net.xinshi.pigeon.dumpload.loaddata.PigeonLoad" %>

<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
    <title>backup</title>
    <style type="text/css">
        ul {
            list-style-type: none;
        }

        li {
            margin-top: 13px;
        }

        #dataDir {
            height: 22px;
            width: 400px;
        }
    </style>
</head>

<body>

<form action="load.jsp" method="post" name="addMerchForm">
    <%
        HttpServletRequest req = (HttpServletRequest) request;
        HttpServletResponse res = (HttpServletResponse) response;
        String dataDir = request.getParameter("dataDir");

        try {
            if (dataDir == null || dataDir.equals("")) {

    %>
    <p>
    <ul>
        <li>导入数据的目录 <input type="text" id="dataDir" name="dataDir" value=""/></li>
        <li><input type="submit" value="导入数据"/></li>
    </ul>
    </p>
    <%
            } else {
                if (StaticPigeonEngine.pigeon instanceof NormalPigeonEngine) {
                    long mt = System.currentTimeMillis();
                    PigeonLoad.doit(((NormalPigeonEngine) StaticPigeonEngine.pigeon).getPigeonStoreEngine(), dataDir);
                    long st = System.currentTimeMillis();
                    out.println("time : " + (st - mt) + " ms");
                } else {
                    out.println("StaticPigeonEngine.pigeon instanceof NormalPigeonEngine == false");
                }
            }
        } catch (Exception e) {
            out.println(e.getMessage());
        }
    %>

</form>

</body>
</html>


