package org.easytool.util;

import org.apache.commons.dbutils.DbUtils;
import org.easytool.dbcompare.model.DB;
import org.easytool.dbcompare.model.DBUrl;
import org.easytool.dbcopy.model.Table;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 复制数据库
 * 1、
 */
public class CopyDB {
	
	public static int write_log = 0;
	
	public static int batchRow = 5000;
	public static int batchRow_100w = 1000000;
	

	/**
	 * 复制 db2->mssqlserver
	 * @param args :0-表名,1-中途提交数量,2-日志提交更新数量,3-源条件部份(不含where)
	 * @throws Exception
	 */
    public static void main(String[] args)  throws Exception{

    	if (args.length>1){
    		try {
				int row = Integer.valueOf(args[1]);
				if (row>1 && row < 5000000){
					batchRow = row;
				}
			} catch (Exception e) {
			}
    		
    		if (args.length>2){
	    		try {
					int row = Integer.valueOf(args[2]);
					if (row>1 && row < 50000000){
						batchRow_100w = row;
					}
					
				} catch (Exception e) {
				}
    		}
    	}
    	String srcWhere = "";
    	if (args.length>3){
    		try {
    			srcWhere = args[3].replace("\"", ""); 
			} catch (Exception e) {
			}
		}
        testCopyTabs(args[0],srcWhere);
        
        if(dbLogConn!=null){
        	dbLogConn.close();
        	dbLogConn = null; 
        }
    }
    
    public static DB dbLogDB = new DB("fssbcs", "db2", "fssb", "Fscs@0901"
            , new DBUrl("db2", "fssbcs", "200.30.10.101", "60000"));
    public static Connection dbLogConn = null;    
    public static Connection getLogConn() throws Exception{
    	synchronized (dbLogDB) {			
    		if (dbLogConn==null){
    			dbLogConn = dbLogDB.getConn();
    		}
		}
    	return dbLogConn;
    }
    
    
    public static void testCopyTabs(String Tab,String srcWhere) throws Exception{

        DB src = new DB("fsylgz", "db2", "zkb_gzyh", "Systemgz3650"
                , new DBUrl("db2", "fsylgz", "200.30.20.24", "50000"));

        DB desc = new DB("sj_2019", "mssql2005", "sj2019", "sj#2019"
                , new DBUrl("mssql2005", "sj_hx1_ylgz_src_2019", "localhost", "1433"));

        String srcTab = Tab;
        String descTab = srcTab;
        
        copyTab(src,desc,srcTab,descTab,srcWhere);
    }        
//        LinkedBlockingQueue tabs = new LinkedBlockingQueue<Table[]>();
//        Table[] tab1= new Table[2];
//        tab1[0] = new Table();
//        tab1[1] = new Table();
//        tabs.add(tab1);
//
//        ThreadPoolExecutor executor = new ThreadPoolExecutor(12, 500, 200, TimeUnit.MILLISECONDS,
//                // new ArrayBlockingQueue<Runnable>(1000)
//                new LinkedBlockingQueue<Runnable>()
//        );
//
//        for(int i=0;i<28;i++){
//            MyTask myTask = new CopyDB.MyTask(i);
//            executor.execute(myTask);
//            System.out.println("线程池中线程数目："+executor.getPoolSize()+"，队列中等待执行的任务数目："+
//                    executor.getQueue().size()+"，已执行完别的任务数目："+executor.getCompletedTaskCount());
//        }
//        executor.shutdown();
//
//    }
//
//    static class MyTask implements Runnable {
//        private int taskNum;
//
//        public MyTask(int num) {
//            this.taskNum = num;
//        }
//
//        @Override
//        public void run() {
//            System.out.println("正在执行task "+taskNum);
//            try {
//                Thread.currentThread().sleep(4000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            System.out.println("task "+taskNum+"执行完毕");
//        }
//    }
    public static boolean copyTab(DB src,DB desc,String srcTab,String descTab,String srcWhere) throws Exception {
        Connection srcConn = src.getConn();
        Connection descConn = desc.getConn();
        long start = System.currentTimeMillis();
        boolean isdb2 = src.getDriver().contains("jdbc:db2");
        boolean isSucc = false;
        long[] count = new long[]{0,0};
        String mess = "";
        try {

        	ins_log(0,src,srcTab,0,start,0,0,false,"",srcWhere);
            // 对比表结构
            Statement stmt = srcConn.createStatement();
            ResultSetMetaData rsd = null;
            try{
                ResultSet rs = stmt.executeQuery("select * from " + srcTab + " where 1=2"+(isdb2?" for read only with ur":""));
                rsd = rs.getMetaData();
                //DbUtils.closeQuietly(rs);
            }catch (Exception e01){
                mess = "源表不存在";
                return false;
            }
            if(write_log==1){
	            try{
	            	Connection logConn = getLogConn();
	            	Statement logstmt = logConn.createStatement();
	                ResultSet rs = logstmt.executeQuery("select clzt from sjwh.to_sj_2019 " +
	                		"where tab='"+srcTab+"' " 
	                		+ ( (srcWhere!=null && srcWhere.trim().length()>0)?
	                				" and tj='" + srcWhere.replace("'","''")+"'":"")
	                		+"order by kssj desc fetch first 1 row only with ur");
	                if(rs.next()){
	                	int clzt = rs.getInt(1);
	                	if(clzt==1 || clzt==2){
	                		// 0-初始,1-成功,2-中途
	                		mess = "日志表最后记录 clzt=1-成功或2-中途 ，请先删除日志表或更新clzt为0-初始或3-失败";
	                		return false;
	                	}
	                }
	                JdbcUtils.closeQuietlys(rs,logstmt); 
	            }catch (Exception e01){
	                mess = "日志表查询失败!";
	                return false;
	            }
            }
            
            Statement stmt2 = descConn.createStatement();
            ResultSetMetaData rsd2 = null;
            try{
                ResultSet rs2 = stmt2.executeQuery("select * from " + descTab + " where 1=2");
                rsd2 = rs2.getMetaData();
                //DbUtils.closeQuietly(rs2);
            }catch (Exception e01){
                mess = "目标表不存在";
                return false;
            }

            boolean isrun = JdbcUtils.isSameMetaData(rsd, rsd2, false);
            JdbcUtils.closeQuietlys(stmt,stmt2);
            if (!isrun) {
                mess = "数据结构不相同";
            } else {
                count = loadData(srcConn, srcTab, descConn, descTab, src, start,srcWhere);
                isSucc = count[1] == 0;
            }
        }catch (Throwable ex){
            mess = "异常："+ex.getMessage();
            ex.printStackTrace();
        } finally {
            JdbcUtils.closeQuietlys(descConn,srcConn);
            long end = System.currentTimeMillis();
            long ys = (end-start);
            long pj_s = ys/1000;
            if(pj_s<=0) pj_s = 1;
            
            String info = "";
            if (isSucc) {
                info = String.format("%s 处理成功! 数量=%d 用时:%d 毫秒(%d秒) 平均:%d 行/每秒"
                        ,srcTab,count[0],ys,pj_s,count[0]/pj_s);
                System.out.println(info);
            }else{
                // error
                info = String.format(srcTab + " 处理失败! %s 已有中途提交数量=%d 用时:%d毫秒(%d秒) 平均:%d 行/每秒"
                        ,mess,count[0],ys,pj_s,count[0]/pj_s);
                System.out.println( info );
            }
            ins_log(1,src,srcTab,count[0],start,end,ys,isSucc,info,srcWhere);
        }
        return isSucc;
    }
    
    public static void ins_log(int lx,DB src,String tab,long sl,long ks,long js,long ys
    		,boolean isSucc,String info,String srcWhere) throws Exception{
    	// create table sjwh.to_sj_2019(tab varchar(128),sl bigint,ks bigint,js bigint,ys bigint,clzt int,kssj timestamp default current timestamp,jssj timestamp,ys_s bigint ,info clob,tj varchar(300));
    	
    	if (write_log!=1){
    		return;
    	}
    	
    	boolean hastj = (srcWhere!=null && srcWhere.trim().length()>0);
    	String sql = "";
    	if(lx==0){ // 初始
    		 sql = "insert into sjwh.to_sj_2019(tab,ks,tj) values(?,?,?)";
    	}else if (lx==1){ // 结束
    		sql = "update sjwh.to_sj_2019 a set sl=?,js=?,ys=?,clzt=?,info=? " +
    				" ,jssj=current timestamp,ys_s=timestampdiff(2, char(current timestamp - kssj))"+
    				" where tab=? and ks=?" + (hastj?" and tj=?":"");
    	}else if (lx==2){ // 中途
    		sql = "update sjwh.to_sj_2019 a set sl=?,js=?,ys=?,clzt=?,info=? " +
    				" ,jssj=current timestamp,ys_s=timestampdiff(2, char(current timestamp - kssj))"+
    				" where tab=? and ks=?" + (hastj?" and tj=?":"");
    	}
    	Connection conn =  getLogConn();;
    	try {
    		//conn = src.getConn(); 
    		PreparedStatement stmt = conn.prepareStatement(sql);
    		if(lx==0){
    			stmt.setString(1,tab);stmt.setLong(2,ks);stmt.setString(3,srcWhere);
    		}else if(lx==1 || lx==2){
    			
    			if(lx==2){
    				js = System.currentTimeMillis();
    	            ys = (js-ks);
    	            long pj_s = ys/1000;
    	            if(pj_s<=0) pj_s = 1;
    	            info += String.format(", 行:%d ,%d 秒,平均:%d 行/每秒"
                    		,sl,pj_s,(sl/pj_s));
    			}
    			
    			stmt.setLong(1,sl);	    		
	    		stmt.setLong(2,js);stmt.setLong(3,ys);
	    		int clzt = 0;
	    		if(isSucc) clzt=1;
	    		if(lx==2) clzt=2;
	    		stmt.setInt(4,clzt);
	    		stmt.setString(5,info);
	    		stmt.setString(6,tab);
	    		stmt.setLong(7,ks);
	    		if(hastj){
		    		stmt.setString(8,srcWhere);
	    		}
	
    		}
    		stmt.execute();    		
		} catch (Exception e) {
			System.out.println("lx="+lx + ",sql="+sql);
			e.printStackTrace();
		}finally{
			//JdbcUtils.closeQuietlys(conn);
		}
    	
    }

    public static boolean compareMeteData(Connection srcConn,String srcTab
            ,Connection descConn,String descTab) throws Exception {

        return false;
    }

    /**
     * 执行数据迁移
     * @param srcConn 源库
     * @param srcTab 源表
     * @param descConn 目标库
     * @param descTab 目标表
     * @return 迁移数量数组 ,[0] 已提交数量,[1] 是否错误(0 正常,-1 异常)
     * @throws Exception
     */
    public static long[] loadData(Connection srcConn,String srcTab
        ,Connection descConn,String descTab,DB src,long start,String srcWhere) throws Throwable {

        Statement stmt = srcConn.createStatement();
        String srcSql = "select * from " + srcTab;
        if(srcWhere!=null && srcWhere.trim().length()>0){
        	srcSql += " where " + srcWhere;
        }
        if(src.getDriver().contains("jdbc:db2")){
        	srcSql = srcSql + " for read only with ur";
        }
        System.out.println("sql:"+srcSql);
        ResultSet rs = stmt.executeQuery(srcSql);
        int colCount = rs.getMetaData().getColumnCount();

        int[] colTypes = JdbcUtils.getColumnTypes(descConn,"select * from " + descTab + " where 1=2");
        if(colCount!=colTypes.length){
            // 再次对比，不相同的则不再继续
            DbUtils.closeQuietly(rs);
            throw new RuntimeException("源与目标字段不相同");
        }
//        for (int i = 0; i < colTypes.length; i++) {
//			System.out.println(i + " : " + colTypes[i]);
//		}
        descConn.setAutoCommit(false);

        String insertSql = String.format("INSERT INTO %s VALUES(%s)"
                , descTab, JdbcUtils.getParamsPair(rs.getMetaData()));
        PreparedStatement descStmt = descConn.prepareStatement(insertSql);

        long count = 0; long commitCount = 0;
        try{
            while(rs.next()){
                for(int i=1; i<=colCount; i++){
                    if(rs.getObject(i) == null){  
                    	//System.out.println(i + " " + colTypes[i-1]);
                        descStmt.setNull(i,colTypes[i-1]);
                        continue;
                    }
                    //System.out.print(rs.getObject(i)+" ");
                    descStmt.setObject(i,rs.getObject(i));
                }
                //System.out.println();
                count++;
                descStmt.addBatch();
                if(count%batchRow == 0){
                    descStmt.executeBatch();
                    descConn.commit();
                    commitCount = count;
    	            long pj_s = (System.currentTimeMillis()-start)/1000;
    	            if(pj_s<=0) pj_s = 1;
                    System.out.println(String.format("行:%d ,%d 秒,平均:%d 行/每秒"
                    		,commitCount,pj_s,(commitCount/pj_s)));
                }
                if(count%batchRow_100w==0){
                	ins_log(2,src,srcTab,count,start,0,0,false,"中途提交",srcWhere);
                }
            }
            if(count%batchRow != 0){
                descStmt.executeBatch();
                descConn.commit();
                commitCount = count;
	            long pj_s = (System.currentTimeMillis()-start)/1000;
	            if(pj_s<=0) pj_s = 1;
                System.out.println(String.format("行:%d ,%d 秒,平均:%d 行/每秒 , 结束"
                		,commitCount,pj_s,(commitCount/pj_s)));
            }
            return new long[]{commitCount,0};
        }catch (Throwable ex){  
        	System.out.println("出错：" + ex.getClass());
        	System.out.println("出错：" + ex.getMessage());
            try{descConn.rollback();}catch(Exception ex1){};
            ex.printStackTrace();
            if(ex instanceof RuntimeException && commitCount == 0){
                throw ex;
            }
        } finally {
            JdbcUtils.closeQuietlys(descStmt,rs,stmt);
        }
        return new long[]{commitCount,-1L};
    }


    /**
     * 按字段名称相同才处理 复制数据
     * @param srcConn
     * @param srcTab
     * @param descConn
     * @param descTab
     * @param src
     * @param start
     * @param srcWhere
     * @return
     * @throws Throwable
     */
    public static long[] loadDataOfCols(Connection srcConn,String srcTab
        ,Connection descConn,String descTab,DB src,long start,String srcWhere
        ) throws Throwable {
    	

        Statement stmt = srcConn.createStatement();
        String srcSql = "select * from " + srcTab;
        if(srcWhere!=null && srcWhere.trim().length()>0){
        	srcSql += " where " + srcWhere;
        }
        if(src.getDriver().contains("jdbc:db2")){
        	srcSql = srcSql + " for read only with ur";
        }
        System.out.println("sql:"+srcSql);
        ResultSet rs = stmt.executeQuery(srcSql);
        ResultSetMetaData src_rds = rs.getMetaData();
        int colCount = src_rds.getColumnCount();

        //int[] colTypes = JdbcUtils.getColumnTypes(descConn,"select * from " + descTab + " where 1=2");
        //if(colCount!=colTypes.length){
        //    // 再次对比，不相同的则不再继续
        //    DbUtils.closeQuietly(rs);
        //    throw new RuntimeException("源与目标字段不相同");
        //}
        String desc_nodata_sql = "select * from " + descTab + " where 1=2";
        ResultSetMetaData desc_rsd = descConn.createStatement().executeQuery(desc_nodata_sql).getMetaData();
        int descCount = desc_rsd.getColumnCount();
        int[] colTypes = new int[colCount];    // 长度是源表的所有列
        int[] descColIndex = new int[colCount];
        String[] descColName = new String[colCount];
        int copy_col_sl = 0;
        for(int i=1; i<=colCount;i++){
        	String src_col_name = src_rds.getColumnName(i);
        	int src_col_type = src_rds.getColumnType(i);
	        for(int j=1; j<=descCount;j++){
	        	if(src_col_name.equals(desc_rsd.getColumnName(j))){
	        		copy_col_sl ++;
	        		descColIndex[i-1] = j;
	        		descColName[i-1] = desc_rsd.getColumnName(j); 
	        		colTypes[i-1] = desc_rsd.getColumnType(j);
	        		if (!JdbcUtils.isSameJdbcType(src_col_type, desc_rsd.getColumnType(j), true)){
	        			throw new RuntimeException(String.format("按字段名称匹配，源与目标字段类型不兼容  字段:%s 源类型=%d 目标类型=%d, 不处理!"
	        					,src_col_name,src_col_type,desc_rsd.getColumnType(j)));
	        		}
	        	}
	        }
        }
        if (copy_col_sl==0){
        	throw new RuntimeException("按字段名称匹配，源与目标字段没有相同的列, 不处理!");
        }
        
        System.out.println(String.format("  源字段数量:%d , 源与目标字段相同(按字段名):%d, 目标字段总数量:%d",colCount, copy_col_sl,descCount));
        
        
//        for (int i = 0; i < colTypes.length; i++) {
//			System.out.println(i + " : " + colTypes[i]);
//		}
        descConn.setAutoCommit(false);
        
        String descObj = "";
        String descFlag = "";
//        if(descColName!=null && descColName.length>0){
        	StringBuilder sb = new StringBuilder();
        	StringBuilder sb_flag = new StringBuilder();
            boolean has = false; // 是否开始有数据     	
        	for (int i = 0; i < descColName.length; i++) {
        		if (descColName[i]!=null){
        			if (has) {
        				sb.append(",");
    					sb_flag.append(",");
        			}else{
        				has = true;
        			}
					sb.append(descColName[i]);
					sb_flag.append("?");
        		}
			}
        	descObj = sb.toString();
        	descFlag = sb_flag.toString();
//        }
//        else{
//        	descFlag = JdbcUtils.getParamsPair(rs.getMetaData());
//        }
        System.out.println("  目标字段列表:"+descObj);

        String insertSql = String.format("INSERT INTO %s%s VALUES(%s)"
                ,descTab, descObj, descFlag);
        PreparedStatement descStmt = descConn.prepareStatement(insertSql);

        if (descColIndex!=null && descColIndex.length>0){
        	if(descColIndex.length<colCount){
        		throw new RuntimeException("提供的目标字段对照索引数量 小于 源列 数量");
        	}
        }

    	// descColName 目标存在的可处理列表 , 此列表 长度是  src.colcount ，不在源存在的 内容为 null
    	// descColIndex 目标存在的列与源列对应，此列表 长度是 src.colcount
        // colTypes 目标存在的列类型与源列对应，此列表 长度是 src.colcount ，索引应与 descColIndex 相同
        
        long count = 0; long commitCount = 0;
        try{
            while(rs.next()){
                for(int i=1; i<=colCount; i++){
                    if(rs.getObject(i) == null){  
                    	//System.out.println(i + " " + colTypes[i-1]);
                        descStmt.setNull(descColIndex[i-1],colTypes[i-1]);
                        continue;
                    }
                    //System.out.print(rs.getObject(i)+" ");
                    descStmt.setObject(descColIndex[i-1],rs.getObject(i));
                }
                //System.out.println();
                count++;
                descStmt.addBatch();
                if(count%batchRow == 0){
                    descStmt.executeBatch();
                    descConn.commit();
                    commitCount = count;
    	            long pj_s = (System.currentTimeMillis()-start)/1000;
    	            if(pj_s<=0) pj_s = 1;
                    System.out.println(String.format("行:%d ,%d 秒,平均:%d 行/每秒"
                    		,commitCount,pj_s,(commitCount/pj_s)));
                }
                if(count%batchRow_100w==0){
                	ins_log(2,src,srcTab,count,start,0,0,false,"中途提交",srcWhere);
                }
            }
            if(count%batchRow != 0){
                descStmt.executeBatch();
                descConn.commit();
                commitCount = count;
	            long pj_s = (System.currentTimeMillis()-start)/1000;
	            if(pj_s<=0) pj_s = 1;
                System.out.println(String.format("行:%d ,%d 秒,平均:%d 行/每秒 , 结束"
                		,commitCount,pj_s,(commitCount/pj_s)));
            }
            return new long[]{commitCount,0};
        }catch (Throwable ex){  
        	System.out.println("出错：" + ex.getClass());
        	System.out.println("出错：" + ex.getMessage());
            try{descConn.rollback();}catch(Exception ex1){};
            ex.printStackTrace();
            if(ex instanceof RuntimeException && commitCount == 0){
                throw ex;
            }
        } finally {
            JdbcUtils.closeQuietlys(descStmt,rs,stmt);
        }
        return new long[]{commitCount,-1L};
    }

}
