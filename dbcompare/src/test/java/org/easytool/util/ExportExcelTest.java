package org.easytool.util;

import junit.framework.TestCase;
import org.easytool.dbcompare.model.DB;
import org.easytool.dbcompare.model.DBUrl;

import java.io.File;
import java.sql.Connection;

public class ExportExcelTest extends TestCase{

    private void toExcel(Connection conn,String sql,String outFileName
            ,String outSheetName,boolean isDelFile) throws Exception {
        File f = new File(outFileName);
        if (f.exists()){
            if (isDelFile) {
                boolean isok = f.delete();
                System.out.printf("删除文件：" + outFileName + " = " + isok + "\n");
                f = null;
                Thread.sleep(1000);
            }else{
                throw new RuntimeException(String.format("已存在文件%s",outFileName));
            }
        }

        ExportExcel.toExcel(sql,conn,outFileName,outSheetName);
    }
    public void testCreateXlsx() throws Exception {
        String dbName = "db2_test2_local";
        dbName="fssbcs";
        //sql="select * from syscat.columns";

        DB db = DbUtilTest.testDB.get(dbName);
        Connection conn = db.getConn();
        try{
            // tables
            String outType = "table";
            String sql = "select a.type,trim(a.TABSCHEMA) ms,a.TABNAME mc\n" +
                    "  ,a.CREATE_TIME,a.COLCOUNT,a.CARD,a.REMARKS \n" +
                    "from syscat.tables a \n" +
                    "where a.tabschema not like 'SYS%' and a.TABNAME not like 'EXPLAIN%'\n" +
                    "order by a.type,ms,mc\n";
            String fileName = String.format("d:/%s_%s.xlsx", dbName,outType);
            String sheetName = "table";
            toExcel(conn,sql,fileName,sheetName,true);

            // col
            outType = "column";
            sql = "select a.type,trim(a.TABSCHEMA) ms,a.tabname mc,a.REMARKS\n" +
                    "  ,b.colno,b.colname,b.remarks,b.TYPENAME,b.NULLS,b.LENGTH,b.SCALE ,b.default,b.keyseq,b.IDENTITY,b.GENERATED\n" +
                    "from syscat.tables a,syscat.columns b \n" +
                    "where a.tabschema not like 'SYS%' and a.TABNAME not like 'EXPLAIN%'\n" +
                    "  and a.TABSCHEMA=b.TABSCHEMA and a.TABNAME=b.TABNAME\n" +
                    "order by a.type,ms,mc,b.colno\n";
            fileName = String.format("d:/%s_%s.xlsx", dbName,outType);
            sheetName = "column";
            toExcel(conn,sql,fileName,sheetName,true);
        }finally {
            if(conn!=null) conn.close();
        }
    }

}
