package org.easytool.dbcompare.model;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.easytool.util.StringUtils;

public class DB {
    public static String CONFIG_PREFIX = ".db";
    public static String NAME = "name";
    public static String TYPE = "type"; // db2/mssql2000/mssql2005/oracle/oracle-tns
    public static String DRIVER = "driver";
    public static String USER = "user";
    public static String PWD = "pwd";
    public static String URL_TYPE = "url_type";
    public static String URL_DBNAME = "url_dbname";
    public static String URL_IP = "url_ip";
    public static String URL_PORT = "url_port";
    public static String URL_FULL = "url_full";
    /**
     * 驱动列表
     */
    private static Map<String,String> DRIVERS = new HashMap<String, String>(){
        {
            put("db2", "com.ibm.db2.jcc.DB2Driver");
            put("oracle", "oracle.jdbc.driver.OracleDriver");
            put("mssql2000", "com.microsoft.jdbc.sqlserver.SQLServerDriver");
            put("mssql2005", "com.microsoft.sqlserver.jdbc.SQLServerDriver");
        }
    };
    private String name;
    private String dbType;
    private String driver;
    private DBUrl url;
    private String user;
    private String pwd;


    /**
     * 数据库配置
     * @param name 名称，唯一标识一个数据库联接 ，名称决定驱动的url部份
     * @param dbType 类型：db2/oracle ，类型决定驱动的driver部份
     * @param user 用户名
     * @param pwd 密码
     */
    public DB(String name,String dbType,String user,String pwd,DBUrl url){
        this.name=name;
        this.user=user;
        this.pwd=pwd;
        this.dbType=dbType;
        this.url= url;
        this.driver=DB.DRIVERS.get(this.dbType);
    }

    public Connection getConn() throws Exception {
        Class.forName(driver);
        Connection conn = DriverManager.getConnection(url.getUrlFull(), user, pwd);
        return conn;
    }

	public static Map<String, String> getDRIVERS() {
		return DRIVERS;
	}

	public static void setDRIVERS(Map<String, String> dRIVERS) {
		DRIVERS = dRIVERS;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDbType() {
		return dbType;
	}

	public void setDbType(String dbType) {
		this.dbType = dbType;
	}

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	public DBUrl getUrl() {
		return url;
	}

	public void setUrl(DBUrl url) {
		this.url = url;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPwd() {
		return pwd;
	}

	public void setPwd(String pwd) {
		this.pwd = pwd;
	}
    

    public static DB getDBformConfig(String configFileName,String configDBName) throws Exception {

    /* config.properties
       srcdb.db.name=fssbcs
       srcdb.db.type=db2
       srcdb.db.driver=
       srcdb.db.url_full=  ##jdbc:db2://{#ip#}:{#port#}/{#dbname#}
       srcdb.db.url_type=db2
       srcdb.db.user=fssb
       srcdb.db.pwd=pwd

       srcdb.db.url_dbname=fssbcs
       srcdb.db.url_ip=200.30.10.101
       srcdb.db.url_port=50000
       */
        Properties prop = new Properties();
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(configFileName);
        prop.load(in);
        DBUrl url = null;
        String url_full = StringUtils.getStringDef(prop.getProperty(configDBName+CONFIG_PREFIX+DB.URL_FULL),"");
        if(StringUtils.isEmpty(url_full)){
            url = new DBUrl(prop.getProperty(configDBName+CONFIG_PREFIX+DB.URL_TYPE)
                    , prop.getProperty(configDBName+CONFIG_PREFIX+DB.URL_DBNAME)
                    , prop.getProperty(configDBName+CONFIG_PREFIX+DB.URL_IP)
                    , prop.getProperty(configDBName+CONFIG_PREFIX+DB.URL_PORT));
        }else{
            url = new DBUrl(prop.getProperty(configDBName+CONFIG_PREFIX+DB.URL_TYPE)
                    ,prop.getProperty(configDBName+CONFIG_PREFIX+DB.URL_DBNAME)
                    ,url_full);
        }
        DB db = new DB(prop.getProperty(configDBName+CONFIG_PREFIX+DB.NAME)
                    , prop.getProperty(configDBName+CONFIG_PREFIX+DB.TYPE)
                    , prop.getProperty(configDBName+CONFIG_PREFIX+DB.USER)
                    , prop.getProperty(configDBName+CONFIG_PREFIX+DB.PWD)
                    , url);
        String driver = StringUtils.getStringDef(prop.getProperty(configDBName+CONFIG_PREFIX+DB.DRIVER),"");
        if(StringUtils.isNotEmpty(driver)){
            db.setDriver(driver);
        }
        return db;
//        return new DB("fssbcs", "db2", "fssb", "Fscs@0901"
//                , new DBUrl("db2", "fssbcs", "200.30.10.101", "60000"));

    }
    
}
