package org.easytool.util;

import junit.framework.TestCase;
import org.easytool.dbcompare.model.DB;
import org.easytool.dbcompare.model.DBUrl;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class ExportExcelTest extends TestCase{
	
	static String temp_path = "E:/_Liqj/myLocal_佛山社保新项目/维护单-总/201903/20190311_统计浮动费率数据201701-201812/";
	static String temp_file = "工伤保险浮动费率数据统计表（社保数据）（201701-201812）_tempfile.xlsx";
	static String out_file_nojf = "工伤保险浮动费率数据统计表（社保数据）（201701-201812）_无缴费_20190312.xlsx";
	static String out_file_jf = "工伤保险浮动费率数据统计表（社保数据）（201701-201812）_有缴费_20190315.xlsx";
	static String sql_no_jf ="select row_number() over(order by a.GSHYLB_dm,value(a.zjl,0),a.sbjgdm,a.dwbh) n\n" + 
			"  ,a.sbjgdm||value('-'||f_gg_mc('sbjg','',a.sbjgdm),'') sbjgdm\n" + 
			"  ,a.dwlx||value('-'||f_gg_jdmb('dwlx',a.dwlx),'') dwlx    \n" + 
			"  --,case when value(a.gshylb_sjfl,'')!='' then \n" + 
			"  --   value(f_gg_jdmb('gshylb',a.gshylb_sjfl),a.gshylb_sjfl)\n" + 
			"  --   else value(f_gg_jdmb('gshylb',a.gshylb_jz),a.gshylb_jz) end  \n" + 
			"  ,value(f_gg_jdmb('gshylb',a.gshylb_jz),a.gshylb_jz) gshylb_缴费类别_征收\n" + 
			"  ,value(f_gg_jdmb('gshylb',a.gshylb),a.gshylb) gshylb_缴费类别_浮动\n" +  // 2019-03-15 新增 
			"  ,value(f_gg_jdmb('gshylb',a.GSHYLB_dm),a.GSHYLB_dm) GSHYLB_dm_实际类别_新\n" + 
			"  ,a.dwbh,a.dwmc\n" + 
			"  --,value(f_gg_jdmb('gshylb',a.gshylb_jz),a.gshylb_jz) \n" + 
			"  ,a.GSHYLB_FL*100 当前执行费率,a.jzfl 基准费率\n" + 
			"  ,case when a.jzfl is not null then a.fddc_fl end fddc_fl_浮动后费率\n" + 
			"  ,a.fddc,a.gw_rs,a.gw_fddc,a.zjl_fddc,value(a.zjl,0)*100 zjl\n" + 
			"  ,a.zsjehj,a.zfjehj,a.ylf,a.kfyl,a.zykf,a.fzqj,a.hsbz,a.jtss,a.ylfhj\n" + 
			"  ,a.scbz,a.ylbz,a.schj,a.szbz,a.gwbz,a.gwbc,a.gwhj,a.qt\n" + 
			"  -- select count(1) c\n" + 
			"from sjwh.YZ_GSBXFDFLSJTJ_20190312 a\n" + 
			"where  value(a.zsjehj,0)<=0 --and a.dwbh='1010225'\n" + 
			"order by a.GSHYLB_dm,value(a.zjl,0),a.sbjgdm,a.gshylb,a.dwbh for read only with ur\n";;
	
	static String sql_jf = "select row_number() over(order by a.GSHYLB_dm,value(a.zjl,0),a.sbjgdm,a.dwbh) n\n" + 
			"  ,a.sbjgdm||value('-'||f_gg_mc('sbjg','',a.sbjgdm),'') sbjgdm\n" + 
			"  ,a.dwlx||value('-'||f_gg_jdmb('dwlx',a.dwlx),'') dwlx    \n" + 
			"  --,case when value(a.gshylb_sjfl,'')!='' then \n" + 
			"  --   value(f_gg_jdmb('gshylb',a.gshylb_sjfl),a.gshylb_sjfl)\n" + 
			"  --   else value(f_gg_jdmb('gshylb',a.gshylb_jz),a.gshylb_jz) end  \n" +
			"  ,value(f_gg_jdmb('gshylb',a.gshylb_jz),a.gshylb_jz) gshylb_缴费类别_征收\n" + 
			"  ,value(f_gg_jdmb('gshylb',a.gshylb),a.gshylb) gshylb_缴费类别_浮动\n" +  // 2019-03-15 新增 
			"  ,value(f_gg_jdmb('gshylb',a.GSHYLB_dm),a.GSHYLB_dm) GSHYLB_dm_实际类别_新\n" + 
			"  ,a.dwbh,a.dwmc\n" + 
			"  --,value(f_gg_jdmb('gshylb',a.gshylb_jz),a.gshylb_jz) \n" + 
			"  ,a.GSHYLB_FL*100 当前执行费率,a.jzfl 基准费率\n" + 
			"  ,case when a.jzfl is not null then a.fddc_fl end fddc_fl_浮动后费率\n" + 
			"  ,a.fddc,a.gw_rs,a.gw_fddc,a.zjl_fddc,value(a.zjl,0)*100 zjl\n" + 
			"  ,a.zsjehj,a.zfjehj,a.ylf,a.kfyl,a.zykf,a.fzqj,a.hsbz,a.jtss,a.ylfhj\n" + 
			"  ,a.scbz,a.ylbz,a.schj,a.szbz,a.gwbz,a.gwbc,a.gwhj,a.qt\n" + 
			"  -- select count(1) c\n" + 
			"from sjwh.YZ_GSBXFDFLSJTJ_20190312 a\n" + 
			"where  a.zsjehj>0 --and a.dwbh='1010225'\n" + 
			"order by a.GSHYLB_dm,value(a.zjl,0),a.sbjgdm,a.gshylb,a.dwbh for read only with ur\n";
	static int startRow = 7;
	
	static String sql_con_1 = "SET CURRENT SCHEMA = \"FSSB\"";
	static String sql_con_2 = "SET CURRENT PATH = \"SYSIBM\",\"SYSFUN\",\"SYSPROC\",\"SYSIBMADM\",\"FSSB\"";
	
	static String db_fssbjdb = "fssbjdb63";
	
	
	public static void execSql(Connection conn,String[] sql) throws Exception{
		Statement stmt = conn.createStatement();
		for (String s : sql) {			
			stmt.execute(s);
		}
		stmt.close();
	}
	
	public static void to_excel_gs() throws Exception{

        DB db = DbUtilTest.testDB.get(db_fssbjdb);
        Connection conn = db.getConn();
        execSql(conn,new String[]{sql_con_1,sql_con_2});
        
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql_jf);
        
        String inFileName = temp_path + temp_file;
        String outFileName = temp_path + out_file_jf;
        String outSheetName = "Sheet1";

        ExportExcel.ResultSetToExeclStartRow(rs, outFileName, outSheetName, startRow, inFileName);
         
	}
	
	
	public static void main(String[] args) throws Exception {
		to_excel_gs();
	}
	
	
	
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
    private void atestCreateXlsx() throws Exception {
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
