<%@ page contentType="text/html;charset=UTF-8" %>
<%@ page import="java.util.List" %>
<%@ page import="net.xinshi.pigeon.saas.util.HostRecord" %>

<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
    <title>show</title>
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

<%
    List<HostRecord> listHosts = HostRecord.handle("/data/httpd-2.4.2/saas_all/show_saas_io");
%>

<br>

<center>

    <font color="red">
        SAAS 数据流量统计
    </font>

    <br>
    <br>

    <table width="50%" border="1" cellpadding="0" cellspacing="0" bordercolor="#000000">
        <tr>
            <th>域 名</th>
            <th>访问次数</th>
            <th>接收字节</th>
            <th>发送字节</th>
        </tr>

        <%
            for (HostRecord hr : listHosts) {
        %>

        <tr>
            <td valign="middle" align="center"><%=hr.getHost()%>
            </td>
            <td valign="middle" align="center"><%=hr.getTimes()%>
            </td>
            <td valign="middle" align="center"><%=hr.getBytes_in()%>
            </td>
            <td valign="middle" align="center"><%=hr.getBytes_out()%>
            </td>
        </tr>

        <%
            }
        %>

    </table>

</center>

</body>
</html>


