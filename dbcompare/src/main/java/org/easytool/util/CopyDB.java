package org.easytool.util;

import org.apache.commons.dbutils.DbUtils;
import org.easytool.dbcompare.model.DB;

import java.sql.*;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 复制数据库
 * 1、
 */
public class CopyDB {
	
	public static int batchRow = 5000;
	public static int batchRow_100w = 1000000;

	/**
	 * 复制 db2->mssqlserver
	 * @param args :0-表名,1-中途提交数量,2-日志提交更新数量,3-分段标志,4-源条件部份(不含where)
	 * @throws Exception
     *
     * start cmd /c call run_one_fd.bat fssb.yz_yldyzfmx 5000 500000 fq8 "xzqydm='440608'"

     @title=%0 %1 %2 %3 %4 %5
     @rem call run_one_fd.bat fssb.ys_grjbxxbg 3000 500000 test1 "grbh='102751995'"
     @rem echo %*
     @rem echo 1=%1 2=%2 3=%3 4=%4 5=%5
     @set path=%path%;E:\sj2019\jdk1.7.0_79\bin\
     @set te=-classpath ".;./lib/*" org.easytool.util.CopyDB -t %1 -r %2 -b %3 -d %4 -w %5
     @set log=logs\output_%1_%2_%4.log
     echo start %date% %time% >>%log%
     echo java %te% >>%log%
     java %te% >>%log% 2>&1
     echo   end %date% %time% >>%log%

	 */
    public static void main(String[] args)  throws Exception{
        Map<String,String> cs = StringUtils.argsToMap(args);
        String tab = StringUtils.getFirstNotEmpty(cs.get("-t"),cs.get("0"));
        if(StringUtils.isEmpty(tab)){
            System.out.println("表名参数必须！ -t 表名 或 第1个参数");
        }
        String row = StringUtils.getFirstNotEmpty(cs.get("-r"));
    	if (StringUtils.isNotEmpty(row)) {
            batchRow = StringUtils.getStrToIntRange(row, 2, 50000, batchRow);
        }
        String brow = StringUtils.getFirstNotEmpty(cs.get("-b"));
        if (StringUtils.isNotEmpty(brow)) {
            batchRow_100w = StringUtils.getStrToIntRange(brow, batchRow, 5000000, batchRow_100w);
        }
    	String fd = StringUtils.getFirstNotEmpty(cs.get("-d"));
    	String srcWhere = StringUtils.getFirstNotEmpty(cs.get("-w"));;
        if (StringUtils.isNotEmpty(srcWhere)) {
    		try {
    			srcWhere = srcWhere.replace("\"", "");
			} catch (Exception e) {
			}
		}
        String configFileName = StringUtils.getFirstNotEmpty(cs.get("-f"),"/db.properties");
        // /db.properties

		if (fd!=null && fd.startsWith("autoFQ") && "".equals(srcWhere)){
		    // 自动分段，且按分区执行
            // -t tab -r 3000 -d autoFQFQBZ1
            String col_fq = fd.substring(4);
            copyTab_FQ_Sbjgdm(configFileName,tab,col_fq);
        }else{
		    testCopyTabs(configFileName,tab,srcWhere,fd);
		}
    }

    public static void copyTab_FQ_Sbjgdm(String configFileName,String tab,String col_fq) throws Exception{
        int minWorkerThreads = 6;
        int maxWorkerThreads = 200;
        ThreadPoolExecutor pool = new ThreadPoolExecutor(minWorkerThreads, maxWorkerThreads, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue()
        );
        DB src = DB.getDBformConfig(configFileName,"srcdb");
        DB desc = DB.getDBformConfig(configFileName,"descdb");
        String[][] fq = new String[][]{ {"fq1","440601"},{"fq4","440604"},{"fq5","440605"}
            ,{"fq6","440606"},{"fq7","440607"},{"fq8","440608"}};
        for (int i = 0; i < fq.length ; i++) {
            String srcWhere = col_fq + "='" + fq[i][1]+"'";
            CopyTask task = new CopyTask(src,desc,tab,tab,srcWhere,fq[i][0]);
            pool.submit(task);
        }
        pool.shutdown();
    }
    public static class CopyTask implements Callable {
        DB src,desc;
        String srcTab,descTab;
        String srcWhere,fd;
        public CopyTask(DB src,DB desc,String srcTab,String descTab,String srcWhere,String fd){
            this.src=src; this.desc=desc;
            this.srcTab = srcTab; this.descTab = descTab; this.srcWhere = srcWhere;
            this.fd = fd;
        }
        public Object call() throws Exception {
            return copyTab(src,desc,srcTab,descTab,srcWhere,fd);
        }
    }



    public static void testCopyTabs(String configFileName,String Tab,String srcWhere,String fd) throws Exception{

//        DB src = new DB("fssbcs", "db2", "fssb", "Fscs@0901"
//                , new DBUrl("db2", "fssbcs", "200.30.10.101", "60000"));
//
//        DB desc = new DB("sj_2019", "mssql2005", "sj2019", "sj#2019"
//                , new DBUrl("mssql2005", "sj_hx1_fssb_src_2019", "localhost", "1433"));

        DB src = DB.getDBformConfig(configFileName,"srcdb");
        DB desc = DB.getDBformConfig(configFileName,"descdb");

        String srcTab = Tab;
        String descTab = srcTab;
        
        copyTab(src,desc,srcTab,descTab,srcWhere,fd);
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
    public static boolean copyTab(DB src,DB desc,String srcTab,String descTab,String srcWhere,String fd) throws Exception {
        boolean srcIsDB2 = src.getUrl().getUrlFull().contains("jdbc:db2:");
        String srcOnlyRead = "";
        if(srcIsDB2){
            srcOnlyRead = " for read only with ur";
        }
        Connection srcConn = src.getConn();
        Connection descConn = desc.getConn();
        long start = System.currentTimeMillis();
        boolean isSucc = false;
        long[] count = new long[]{0,0};
        String mess = "";
        try {

        	ins_log(0,src,srcTab,0,start,0,0,false,"",srcWhere,fd);
            // 对比表结构
            Statement stmt = srcConn.createStatement();
            ResultSetMetaData rsd = null;
            try{
                String sql = "select * from " + srcTab + " where 1=2 " + srcOnlyRead;
                ResultSet rs = stmt.executeQuery( sql);
                rsd = rs.getMetaData();
                //DbUtils.closeQuietly(rs);
            }catch (Exception e01){
                try {
                    ResultSet rs = stmt.executeQuery("select * from " + srcTab + " where 1=2");
                    rsd = rs.getMetaData();
                }catch (Exception e02) {
                    e02.printStackTrace();
                }
                if(rsd ==null ) {
                    mess = "源表不存在";
                    return false;
                }
            }
            try{
                ResultSet rs = stmt.executeQuery("select clzt from sjwh.to_sj_2019 " +
                		"where tab='"+srcTab+"' " 
                		+ ( (srcWhere!=null && srcWhere.trim().length()>0)?
                				" and tj='" + srcWhere.replace("'","''")+"'":"")
                		+" order by kssj desc "
                        +(srcIsDB2?"fetch first 1 row only with ur":""));
                if(rs.next()){
                	int clzt = rs.getInt(1);
                	if(clzt==1 || clzt==2){
                		// 0-初始,1-成功,2-中途
                		mess = "日志表最后记录 clzt=1-成功或2-中途 ，请先删除日志表或更新clzt为0-初始或3-失败";
                		return false;
                	}
                }
                DbUtils.closeQuietly(rs);
            }catch (Exception e01){
                mess = "日志表查询失败!";
                return false;
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
                count = loadData(srcConn, srcTab, descConn, descTab, src, start,srcWhere,srcOnlyRead,fd);
                isSucc = count[1] == 0;
            }
        }catch (Throwable ex){
            mess ="异常："+ex.getMessage();
            ex.printStackTrace();
        } finally {
            JdbcUtils.closeQuietlys(descConn,srcConn);
            long end = System.currentTimeMillis();
            long ys = (end-start);
            long pj_s = ys/1000;
            if(pj_s<=0) pj_s = 1;
            
            String info = "";
            if (isSucc) {
                info = String.format("%s %s 处理成功! 数量=%d 用时:%d 毫秒(%d秒) 平均:%d 行/每秒"
                        ,srcTab,fd,count[0],ys,pj_s,count[0]/pj_s);
                System.out.println(info);
            }else{
                // error
                info = String.format("%s %s 处理失败! %s 已有中途提交数量=%d 用时:%d毫秒(%d秒) 平均:%d 行/每秒"
                        ,srcTab,fd,mess,count[0],ys,pj_s,count[0]/pj_s);
                System.out.println( info );
            }
            ins_log(1,src,srcTab,count[0],start,end,ys,isSucc,info,srcWhere,fd);
        }
        return isSucc;
    }
    
    public static void ins_log(int lx,DB src,String tab,long sl,long ks,long js,long ys
    		,boolean isSucc,String info,String srcWhere,String fd){
    	// create table sjwh.to_sj_2019(tab varchar(128),sl bigint,ks bigint,js bigint,ys bigint,clzt int,kssj timestamp default current timestamp,jssj timestamp,ys_s bigint ,info clob,tj varchar(300));
    	// alter table sjwh.to_sj_2019 add fd varchar(50) add pj_s bigint;
    	boolean hastj = (srcWhere!=null && srcWhere.trim().length()>0);
    	String sql = "";
    	if(lx==0){ // 初始
    		 sql = "insert into sjwh.to_sj_2019(tab,ks,tj,clzt,fd) values(?,?,?,2,?)";
    	}else if (lx==1){ // 结束
    		sql = "update sjwh.to_sj_2019 a set sl=?,js=?,ys=?,clzt=?,info=?,pj_s=? " +
    				" ,jssj=current timestamp,ys_s=timestampdiff(2, char(current timestamp - kssj))"+
    				" where tab=? and ks=?" + (hastj?" and tj=?":"");
    	}else if (lx==2){ // 中途
    		sql = "update sjwh.to_sj_2019 a set sl=?,js=?,ys=?,clzt=?,info=?,pj_s=? " +
    				" ,jssj=current timestamp,ys_s=timestampdiff(2, char(current timestamp - kssj))"+
    				" where tab=? and ks=?" + (hastj?" and tj=?":"");
    	}
    	Connection conn = null;
    	try {
    		conn = src.getConn(); 
    		PreparedStatement stmt = conn.prepareStatement(sql);
    		if(lx==0){
    			stmt.setString(1,tab);
    			stmt.setLong(2,ks);
    			stmt.setString(3,srcWhere);
                stmt.setString(4,fd);
    		}else if(lx==1 || lx==2){
                long pj_s = 0;
    			if(lx==2){
    				js = System.currentTimeMillis();
    	            ys = (js-ks);
    	            pj_s = ys/1000;
    	            if(pj_s<=0) pj_s = 1;
    	            info += String.format(", 行:%d ,%d 秒,平均:%d 行/每秒"
                    		,sl,pj_s,(sl/pj_s));
    			}else{
    			    pj_s = (js-ks)/1000;
                }
    			
    			stmt.setLong(1,sl);	    		
	    		stmt.setLong(2,js);stmt.setLong(3,ys);
	    		int clzt = 3;
	    		if(isSucc) clzt=1;
	    		if(lx==2) clzt=2;
	    		stmt.setInt(4,clzt);
	    		stmt.setString(5,info);
                stmt.setLong(6,pj_s);

	    		stmt.setString(7,tab);
	    		stmt.setLong(8,ks);
	    		if(hastj){
		    		stmt.setString(9,srcWhere);
	    		}
	
    		}
    		stmt.execute();    		
		} catch (Exception e) {
			System.out.println("lx="+lx + ",sql="+sql);
			e.printStackTrace();
		}finally{
			JdbcUtils.closeQuietlys(conn);
		}
    	
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
        ,Connection descConn,String descTab,DB src,long start,String srcWhere,String srcOnlyRead,String fd) throws Throwable {

        Statement stmt = srcConn.createStatement();
        String srcSql = "select * from " + srcTab;
        if(srcWhere!=null && srcWhere.trim().length()>0){
        	srcSql += " where " + srcWhere;
        }
        srcSql = srcSql + srcOnlyRead;
        System.out.println(fd+" sql:"+srcSql);
        ResultSet rs = null;
//        try{
            rs = stmt.executeQuery(srcSql);
//        }catch (Exception e){
//            try{
//                rs = stmt.executeQuery(srcSql.replace(" for read only with ur",""));
//            } catch (Exception e1){
//                e1.printStackTrace();
//                throw new RuntimeException(e1.getMessage());
//            }
//        }
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
                    System.out.println(String.format("%s 行:%d ,%d 秒,平均:%d 行/每秒"
                    		,fd,commitCount,pj_s,(commitCount/pj_s)));
                }
                if(count%batchRow_100w==0){
                	ins_log(2,src,srcTab,count,start,0,0,false,"中途提交",srcWhere,fd);
                }
            }
            if(count%batchRow != 0){
                descStmt.executeBatch();
                descConn.commit();
                commitCount = count;
	            long pj_s = (System.currentTimeMillis()-start)/1000;
	            if(pj_s<=0) pj_s = 1;
                System.out.println(String.format("%s 行:%d ,%d 秒,平均:%d 行/每秒 , 结束"
                		,fd,commitCount,pj_s,(commitCount/pj_s)));
            }
            return new long[]{commitCount,0};
        }catch (Throwable ex){  
        	System.out.println(fd+" 出错：" + ex.getClass());
        	System.out.println(fd+" 出错：" + ex.getMessage());
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
