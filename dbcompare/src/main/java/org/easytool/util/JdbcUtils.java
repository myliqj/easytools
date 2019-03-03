package org.easytool.util;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JdbcUtils {

    /**
     * 获取表的字段类型
     *
     * @param connection
     * @param table
     * @return
     * @throws SQLException
     */
    public static Map<String, Integer> getColumnTypes(Connection connection, String table, String keywordEscaper) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM ");
        sql.append(keywordEscaper);
        sql.append(table);
        sql.append(keywordEscaper);
        sql.append(" WHERE 1=2");
        //sql.append(" Limit 1");

        ResultSetHandler<Map<String, Integer>> handler = new ResultSetHandler<Map<String, Integer>>() {
            public Map<String, Integer> handle(ResultSet rs) throws SQLException {
                Map<String, Integer> map = new HashMap<String, Integer>();
                ResultSetMetaData rsd = rs.getMetaData();
                for (int i = 0; i < rsd.getColumnCount(); i++) {
                    map.put(rsd.getColumnName(i + 1).toLowerCase(), rsd.getColumnType(i + 1));
                }
                return map;
            }
        };

        QueryRunner runner = new QueryRunner();
        return runner.query(connection, sql.toString(), handler);
    }

    /**
     * 获取表的字段名称
     *
     * @param conn
     * @param table
     * @return
     * @throws SQLException
     */
    public static List<String> getColumnNames(Connection conn, String table, String keywordEscaper) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM ");
        sql.append(keywordEscaper);
        sql.append(table);
        sql.append(keywordEscaper);
        sql.append(" WHERE 1=2");
        //sql.append(" Limit 1");

        ResultSetHandler<List<String>> handler = new ResultSetHandler<List<String>>() {
            public List<String> handle(ResultSet rs) throws SQLException {
                List<String> columnNames = new ArrayList<String>();
                ResultSetMetaData rsd = rs.getMetaData();

                for (int i = 0, len = rsd.getColumnCount(); i < len; i++) {
                    columnNames.add(rsd.getColumnName(i + 1));
                }
                return columnNames;
            }
        };

        QueryRunner runner = new QueryRunner();
        return runner.query(conn, sql.toString(), handler);
    }

    /**
     * 查询表中分割字段值的区域（最大值、最小值）
     *
     * @param conn
     * @param sql
     * @param splitColumn
     * @return
     * @throws SQLException
     */
    public static double[] querySplitColumnRange(Connection conn, final String sql, final String splitColumn) throws SQLException {
        double[] minAndMax = new double[2];
        Pattern p = Pattern.compile("\\s+FROM\\s+.*", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(sql);

        if (m.find() && splitColumn != null && !"".equals(splitColumn.trim())) {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT MIN(");
            sb.append(splitColumn);
            sb.append("), MAX(");
            sb.append(splitColumn);
            sb.append(")");
            sb.append(m.group(0));

            ResultSetHandler<double[]> handler = new ResultSetHandler<double[]>() {
                public double[] handle(ResultSet rs) throws SQLException {
                    double[] minAndMax = new double[2];
                    while (rs.next()) {
                        minAndMax[0] = rs.getDouble(1);
                        minAndMax[1] = rs.getDouble(2);
                    }

                    return minAndMax;
                }
            };

            QueryRunner runner = new QueryRunner();
            return runner.query(conn, sb.toString(), handler);
        }

        return minAndMax;
    }

    /**
     * 查询表数值类型的主键
     *
     * @param conn
     * @param catalog
     * @param schema
     * @param table
     * @return
     * @throws SQLException
     */
    public static String getDigitalPrimaryKey(Connection conn, String catalog, String schema, String table, String keywordEscaper)
            throws SQLException {
        List<String> primaryKeys = new ArrayList<String>();
        ResultSet rs = conn.getMetaData().getPrimaryKeys(catalog, schema, table);
        while (rs.next()) {
            primaryKeys.add(rs.getString("COLUMN_NAME"));
        }
        rs.close();

        if (primaryKeys.size() > 0) {
            Map<String, Integer> map = getColumnTypes(conn, table, keywordEscaper);
            for (String pk : primaryKeys) {
                if (isDigitalType(map.get(pk.toLowerCase()))) {
                    return pk;
                }
            }
        }

        return null;
    }

    /**
     * 判断字段类型是否为数值类型
     *
     * @param sqlType
     * @return
     */
    public static boolean isDigitalType(int sqlType) {
        switch (sqlType) {
            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
                return true;
            default:
                return false;
        }
    }
    /**
     * 判断字段类型是否为字符类型
     *
     * @param sqlType
     * @return
     */
    public static boolean isStringType(int sqlType) {
        switch (sqlType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                return true;
            default:
                return false;
        }
    }
    /**
     * 判断字段类型是否为日期时间类型
     *
     * @param sqlType
     * @return
     */
    public static boolean isDateTimeType(int sqlType) {
        switch (sqlType) {
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                return true;
            default:
                return false;
        }
    }
    /**
     * 判断字段类型是否相同或兼容
     *
     * @param t1 源字段类型
     * @param t2 目标字段类型
     * @param isCompatible 是否仅仅判断兼容, true-是,false-否(要完全一至,一般相同数据库才需要false)
     * @return
     */
    public static boolean isSameJdbcType(int t1,int t2,boolean isCompatible){
        /*
        数字 SMALLINT=5, INTEGER=4, BIGINT=-5, FLOAT=6, REAL=7, DOUBLE=8, NUMERIC=2, DECIMAL=3,
        字符 CHAR=1, VARCHAR=12, LONGVARCHAR=-1, NCHAR=-15, NVARCHAR=-9, LONGNVARCHAR=-16,
        日期 DATE=91, TIME=92, TIMESTAMP=93,
        二进制 TINYINT=-6, BIT=-7, BINARY=-2, VARBINARY=-3, LONGVARBINARY=-4, BOOLEAN=16,
        大对像 BLOB=2004, CLOB=2005, NCLOB=2011,
        其它 NULL=0,OTHER=1111,JAVA_OBJECT=2000,DISTINCT=2001,STRUCT=2002,ARRAY=2003,REF=2006,DATALINK=70,ROWID=-8,SQLXML=2009,
        */
        if(!isCompatible) return t1==t2;
        if (JdbcUtils.isDigitalType(t1) && JdbcUtils.isDigitalType(t2)) return true;
        if (JdbcUtils.isStringType(t1) && JdbcUtils.isStringType(t2)) return true;
        if (JdbcUtils.isDateTimeType(t1) && JdbcUtils.isDateTimeType(t2)) return true;
        return t1==t2;
    }

    public static boolean isSameMetaData(ResultSetMetaData r1,ResultSetMetaData r2
            ,boolean isCompatible) throws Exception {
        if(r1.getColumnCount()!=r2.getColumnCount()) return false;
        for (int i = 1; i < r1.getColumnCount(); i++) {
            if (!r1.getColumnName(i).equals(r2.getColumnName(i))
                    && !isSameJdbcType(r1.getColumnType(i),r2.getColumnType(i),isCompatible)){
                // 判断名称是否完全相同，类型是否兼容
                return false;
            }
        }
        return true;
    }

    public static String getParamsPair(ResultSetMetaData r1) throws Exception{
        StringBuilder sb = new StringBuilder(r1.getColumnCount()*2-1);
        for (int i = 0; i < r1.getColumnCount(); i++) {
            if(i>0) sb.append(",");
            sb.append("?");
        }
        return sb.toString();
    }
    public static ResultSetMetaData getResultSetMetaData(Connection conn,String sql) throws Exception{
        return conn.createStatement().executeQuery(sql).getMetaData();
    }
    public static int[] getColumnTypes(Connection conn,String sql) throws Exception{
        ResultSetMetaData rsd = conn.createStatement().executeQuery(sql).getMetaData();
        int[] colTypes = new int[rsd.getColumnCount()];
        for(int i=0; i<colTypes.length-1;i++){
            colTypes[i] = rsd.getColumnType(i+1);
        }
        return colTypes;
    }

    public static Connection getConnection(String driverClassName, String url, String username, String password) throws Exception {
        Class.forName(driverClassName);
        return DriverManager.getConnection(url, username, password);
    }

    public static void closeQuietlys(Object ... obj){
        for(int i=0;i<obj.length;i++){
            Object o = obj[i];
            if (o == null) continue;
            if(o instanceof ResultSet){
                DbUtils.closeQuietly((ResultSet)o);
            }else if(o instanceof Statement){
                DbUtils.closeQuietly((Statement)o);
            }else if(o instanceof Connection){
                DbUtils.closeQuietly((Connection)o);
            }else{
               throw new RuntimeException("类型不正确，只允许 ResultSet,Statement,Connection");
            }
        }
    }

}
