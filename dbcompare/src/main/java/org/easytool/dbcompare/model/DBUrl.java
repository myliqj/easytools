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

    private String urlDbType;
    private String urlFull;
    private String urlIp;
    private String urlPort;
    private String urlDbName;
    /**
     * 唯一确定一个数据库，包括IP+DbName
     * @param urlDbType jdbc数据库类型:db2/oracle ，确定驱动类型 ,指 url_db_type
     * @param urlDbName 数据库名称
     * @param urlIp IP地址
     * @param urlPort 端口
     */
    public DBUrl(String urlDbType,String urlDbName,String urlIp,String urlPort){
        this.urlDbType=urlDbType;
        this.urlDbName=urlDbName;this.urlIp=urlIp; this.urlPort=urlPort;
        this.urlFull = JDBC_URL.get(this.urlDbType).replace("{#dbname#}", this.urlDbName)
                .replace("{#ip#}", this.urlIp).replace("{#port#}", this.urlPort);
    }
    public DBUrl(String urlDbType,String urlDbName,String urlFull){
        this.urlDbType=urlDbType;
        this.urlDbName=urlDbName;
        this.urlFull = urlFull;
    }

    public String getUrlFull() {
        return urlFull;
    }

    public void setUrlFull(String urlFull) {
        this.urlFull = urlFull;
    }

    public String getUrlIp() {
        return urlIp;
    }

    public void setUrlIp(String urlIp) {
        this.urlIp = urlIp;
    }

    public String getUrlPort() {
        return urlPort;
    }

    public void setUrlPort(String urlPort) {
        this.urlPort = urlPort;
    }

    public String getUrlDbName() {
        return urlDbName;
    }

    public void setUrlDbName(String urlDbName) {
        this.urlDbName = urlDbName;
    }

    public String getUrlDbType() {
        return urlDbType;
    }

    public void setUrlDbType(String urlDbType) {
        this.urlDbType = urlDbType;
    }
}
