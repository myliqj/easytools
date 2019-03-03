package org.easytool.dbcompare.model;

import java.util.HashMap;
import java.util.Map;

public class DBUrl {
    /**
     * jdbc 联接串列表,使用  {##} 占位符，有ip/port/dbname
     */
    private static Map<String,String> JDBC_URL = new HashMap<String, String>(){
        {
            put("db2", "jdbc:db2://{#ip#}:{#port#}/{#dbname#}");
            put("oracle", "jdbc:oracle:thin:@{#ip#}:{#port#}:{#dbname#}");
            put("oracle-tns","jdbc:oracle:thin:@(DESCRIPTION =(ADDRESS_LIST =(ADDRESS=(PROTOCOL=TCP)(HOST={#ip#})(PORT={#port#})))(SOURCE_ROUTE = off)(FAILOVER = on)(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME={#dbname#})))");
            put("mssql2000","jdbc:microsoft:sqlserver://{#ip#}:{#port#};DataBaseName={#dbname#}");
            put("mssql2005","jdbc:sqlserver://{#ip#}:{#port#};DataBaseName={#dbname#}");
        }
    };

    private String url;
    private String ip;
    private String port;
    private String dbName;
    /**
     * 唯一确定一个数据库，包括IP+DbName
     * @param dbType jdbc数据库类型:db2/oracle ，确定驱动类型
     * @param dbName 数据库名称
     * @param ip IP地址
     * @param port 端口
     */
    public DBUrl(String dbType,String dbName,String ip,String port){
        this.dbName=dbName;this.ip=ip; this.port=port;
        this.url = JDBC_URL.get(dbType).replace("{#dbname#}", this.dbName)
                .replace("{#ip#}", this.ip).replace("{#port#}", this.port);
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
}
