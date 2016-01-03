<%@ page contentType="text/html;charset=UTF-8" %>
<%@ page import="net.xinshi.pigeon.saas.SaasPigeonEngine" %>

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

        #merchantId {
            height: 22px;
            width: 400px;
        }

        #saveDir {
            height: 22px;
            width: 400px;
        }
    </style>
</head>

<body>

<form action="backup.jsp" method="post" name="addMerchForm">
    <%
        HttpServletRequest req = (HttpServletRequest) request;
        HttpServletResponse res = (HttpServletResponse) response;
        String merchantId = request.getParameter("merchantId");
        String saveDir = request.getParameter("saveDir");

        try {
            if (merchantId == null || merchantId.equals("")) {
                merchantId = null;
            }
            if (saveDir == null || saveDir.equals("")) {

    %>
    <p>
    <ul>
        <li>商家ID(不填表示所有商家) <input type="text" id="merchantId" name="merchantId" value=""/></li>
        <li>保存数据的目录 <input type="text" id="saveDir" name="saveDir" value="/data/isonev45/eMall/backup"/></li>
        <li><input type="submit" value="备份"/></li>
    </ul>
    </p>
    <%
            } else {
                long mt = System.currentTimeMillis();
                String dir = net.xinshi.pigeon.saas.backup.ClientBackup.backup(merchantId, saveDir);
                long st = System.currentTimeMillis();
                out.println("数据备份的目录: " + dir);
                out.println("time : " + (st - mt) + " ms");
            }
        } catch (Exception e) {
        }

    %>

</form>

</body>
</html>


