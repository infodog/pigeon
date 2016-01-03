package net.xinshi.pigeon.config;

/**
 * Created with IntelliJ IDEA.
 * User: WPF
 * Date: 13-8-12
 * Time: 上午10:22
 * To change this template use File | Settings | File Templates.
 */
public class template {

    public static String pigeonserver = "{\n" +
            "    \"pigeons\": [\n" +
            "        {\n" +
            "            \"host\": \"${host}\",\n" +
            "            \"type\": \"flexobject\",\n" +
            "            \"master\": true,\n" +
            "            \"nodeName\": \"${host}_${port}_flexobject${no}\",\n" +
            "            \"instanceName\": \"/flexobject${no}\",\n" +
            "            \"version\": \"${ver}\",\n" +
            "            \"table\": \"t_flexobject\",\n" +
            "            \"logDir\": \"../data/flexobject${no}/\",\n" +
            "            \"maxCacheNumber\": 10000,\n" +
            "            \"dbUrl\": \"jdbc:mysql://127.0.0.1/${dbname}?autoReconnect=true\",\n" +
            "            \"dbUserName\": \"${dbuser}\",\n" +
            "            \"dbPassword\": \"${dbpasswd}\"\n" +
            "        },\n" +
            "        {\n" +
            "            \"host\": \"${host}\",\n" +
            "            \"type\": \"list\",\n" +
            "            \"master\": true,\n" +
            "            \"nodeName\": \"${host}_${port}_list${no}\",\n" +
            "            \"instanceName\": \"/list${no}\",\n" +
            "            \"version\": \"${ver}\",\n" +
            "            \"table\": \"t_listband\",\n" +
            "            \"logDir\": \"../data/list${no}/\",\n" +
            "            \"dbUrl\": \"jdbc:mysql://127.0.0.1/${dbname}?autoReconnect=true\",\n" +
            "            \"dbUserName\": \"${dbuser}\",\n" +
            "            \"dbPassword\": \"${dbpasswd}\"\n" +
            "        },\n" +
            "        {\n" +
            "            \"host\": \"${host}\",\n" +
            "            \"type\": \"atom\",\n" +
            "            \"master\": true,\n" +
            "            \"nodeName\": \"${host}_${port}_atom${no}\",\n" +
            "            \"instanceName\": \"/atom${no}\",\n" +
            "            \"version\": \"${ver}\",\n" +
            "            \"table\": \"t_simpleatom\",\n" +
            "            \"logDir\": \"../data/atom${no}/\",\n" +
            "            \"dbUrl\": \"jdbc:mysql://127.0.0.1/${dbname}?autoReconnect=true\",\n" +
            "            \"dbUserName\": \"${dbuser}\",\n" +
            "            \"dbPassword\": \"${dbpasswd}\"\n" +
            "        },\n" +
            "        {\n" +
            "            \"host\": \"${host}\",\n" +
            "            \"type\": \"idserver\",\n" +
            "            \"master\": true,\n" +
            "            \"nodeName\": \"${host}_${port}_idserver${no}\",\n" +
            "            \"instanceName\": \"/idserver${no}\",\n" +
            "            \"idNumPerRound\": \"10000\",\n" +
            "            \"version\": \"${ver}\",\n" +
            "            \"logDir\": \"../data/idserver${no}\",\n" +
            "            \"dbUrl\": \"jdbc:mysql://127.0.0.1/${dbname}?autoReconnect=true&rewriteBatchedStatements=true\",\n" +
            "            \"dbUserName\": \"${dbuser}\",\n" +
            "            \"dbPassword\": \"${dbpasswd}\"\n" +
            "        },\n" +
            "        {\n" +
            "            \"host\": \"${host}\",\n" +
            "            \"type\": \"lock\",\n" +
            "            \"master\": true,\n" +
            "            \"nodeName\": \"${host}_${port}_lock${no}\",\n" +
            "            \"instanceName\": \"/lock${no}\",\n" +
            "            \"version\": \"${ver}\"\n" +
            "        },\n" +
            "        {\n" +
            "            \"host\": \"${host}\",\n" +
            "            \"type\": \"fileserver\",\n" +
            "            \"master\": true,\n" +
            "            \"nodeName\": \"${host}_${port}_fileserver${no}\",\n" +
            "            \"instanceName\": \"/fileserver1\",\n" +
            "            \"pigeonStoreConfigFile\": \"../conf/pigeonnodes.conf\",\n" +
            "            \"pigeonFileSystemConfigFile\": \"../conf/pigeonfilesystem.conf\",\n" +
            "            \"name\": \"server1\",\n" +
            "            \"baseDir\": \"../files\"\n" +
            "        }\n" +
            "    ]\n" +
            "}";

    public static String pigeonfilesystem = "{\n" +
            "    \"globalgroups\": [\n" +
            "        {\n" +
            "            \"name\": \"g1\",\n" +
            "            \"priority\": 100,\n" +
            "            \"genRelated\": \"no\",\n" +
            "            \"preferredLocalGroup\": \"lg1\",\n" +
            "            \"localgroups\": [\n" +
            "                {\n" +
            "                    \"id\": \"lg1\",\n" +
            "                    \"servers\": [\n" +
            "                        {\n" +
            "                            \"internalUrl\": \"/img\",\n" +
            "                            \"externalUrl\": \"/img\",\n" +
            "                            \"writeUrl\": \"http://${host}:${port}/fileserver1\",\n" +
            "                            \"serverId\": \"server1\"\n" +
            "                        }\n" +
            "                    ]\n" +
            "                }\n" +
            "            ]\n" +
            "        }\n" +
            "    ]\n" +
            "}";

}

