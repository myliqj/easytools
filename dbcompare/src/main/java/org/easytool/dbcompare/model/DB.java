package org.easytool.dbcompare.model;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;

public class DB {
    /**
     * 驱动列表
     */
    private static Map<String,String> DRIVER = new HashMap<String, String>(){
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
        this.driver=DB.DRIVER.get(this.dbType);
    }

    public Connection getConn() throws Exception {
        Class.forName(driver);
        Connection conn = DriverManager.getConnection(url.getUrl(), user, pwd);
        return conn;
    }

	public static Map<String, String> getDRIVER() {
		return DRIVER;
	}

	public static void setDRIVER(Map<String, String> dRIVER) {
		DRIVER = dRIVER;
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
    
    
    
}
