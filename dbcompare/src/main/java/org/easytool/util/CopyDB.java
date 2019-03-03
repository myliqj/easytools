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

    public static void main(String[] args)  throws Exception{

        testCopyTabs();
    }
    public static void testCopyTabs(){

        DB src = new DB("test2", "db2", "db2admin", "db2admin"
                , new DBUrl("db2", "test2", "127.0.0.1", "50000"));

        DB desc = new DB("test2", "db2", "db2admin", "db2admin"
                , new DBUrl("db2", "test2", "127.0.0.1", "50000"));

        String srcTab = "FSSB.MY_MD5";
        String descTab = "FSSB.MY_MD5_New20190303";
        LinkedBlockingQueue tabs = new LinkedBlockingQueue<Table[]>();
        Table[] tab1= new Table[2];
        tab1[0] = new Table();
        tab1[1] = new Table();
        tabs.add(tab1);

        ThreadPoolExecutor executor = new ThreadPoolExecutor(12, 500, 200, TimeUnit.MILLISECONDS,
                // new ArrayBlockingQueue<Runnable>(1000)
                new LinkedBlockingQueue<Runnable>()
        );

        for(int i=0;i<28;i++){
            MyTask myTask = new CopyDB.MyTask(i);
            executor.execute(myTask);
            System.out.println("线程池中线程数目："+executor.getPoolSize()+"，队列中等待执行的任务数目："+
                    executor.getQueue().size()+"，已执行完别的任务数目："+executor.getCompletedTaskCount());
        }
        executor.shutdown();

    }

    static class MyTask implements Runnable {
        private int taskNum;

        public MyTask(int num) {
            this.taskNum = num;
        }

        @Override
        public void run() {
            System.out.println("正在执行task "+taskNum);
            try {
                Thread.currentThread().sleep(4000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("task "+taskNum+"执行完毕");
        }
    }
    public static boolean copyTab(DB src,DB desc,String srcTab,String descTab) throws Exception {
        Connection srcConn = src.getConn();
        Connection descConn = desc.getConn();
        long start = System.currentTimeMillis();
        boolean isSucc = false;
        long[] count = new long[]{0,0};
        String mess = "";
        try {
            // 对比表结构
            Statement stmt = srcConn.createStatement();
            ResultSetMetaData rsd = null;
            try{
                ResultSet rs = stmt.executeQuery("select * from " + srcTab + " where 1=2");
                rsd = rs.getMetaData();
                DbUtils.closeQuietly(rs);
            }catch (Exception e01){
                mess = "源表不存在";
                return false;
            }

            Statement stmt2 = descConn.createStatement();
            ResultSetMetaData rsd2 = null;
            try{
                ResultSet rs2 = stmt2.executeQuery("select * from " + descTab + " where 1=2");
                rsd2 = rs2.getMetaData();
                DbUtils.closeQuietly(rs2);
            }catch (Exception e01){
                mess = "目标表不存在";
                return false;
            }

            boolean isrun = JdbcUtils.isSameMetaData(rsd, rsd2, false);
            JdbcUtils.closeQuietlys(stmt,stmt2);
            if (!isrun) {
                mess = "数据结构不相同";
            } else {
                count = loadData(srcConn, srcTab, descConn, descTab);
                isSucc = count[1] == 0;
            }
        }catch (Throwable ex){
            mess = "异常："+ex.getMessage();
            ex.printStackTrace();
        } finally {
            JdbcUtils.closeQuietlys(descConn,srcConn);
            long end = System.currentTimeMillis();
            long ys = (end-start);
            if (isSucc) {
                System.out.println("处理成功！数量="+count[0] + " 用时：" + ys + "毫秒");
            }else{
                // error
                String info = String.format("处理失败! %s 已有中途提交数量=%d 用时：%d毫秒"
                        ,mess,count[0],ys);
                System.out.println( info );
            }
        }
        return isSucc;
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
        ,Connection descConn,String descTab) throws Exception {

        Statement stmt = srcConn.createStatement();
        ResultSet rs = stmt.executeQuery("select * from " + srcTab);
        int colCount = rs.getMetaData().getColumnCount();

        int[] colTypes = JdbcUtils.getColumnTypes(descConn,"select * from " + descTab + " where 1=2");
        if(colCount!=colTypes.length){
            // 再次对比，不相同的则不再继续
            DbUtils.closeQuietly(rs);
            throw new RuntimeException("源与目标字段不相同");
        }
        descConn.setAutoCommit(false);

        String insertSql = String.format("INSERT INTO %s VALUES(%s)"
                , descTab, JdbcUtils.getParamsPair(rs.getMetaData()));
        PreparedStatement descStmt = descConn.prepareStatement(insertSql);

        long count = 0; long commitCount = 0;
        try{
            while(rs.next()){
                for(int i=1; i<=colCount; i++){
                    if(rs.getObject(i) == null){
                        descStmt.setNull(i,colTypes[i-1]);
                        continue;
                    }
                    descStmt.setObject(i,rs.getObject(i));
                }
                count++;
                descStmt.addBatch();
                if(count%5000 == 0){
                    descStmt.executeBatch();
                    descConn.commit();
                    commitCount = count;
                    System.out.println("写入行:"+commitCount);
                }
            }
            if(count%5000 != 0){
                descStmt.executeBatch();
                descConn.commit();
                commitCount = count;
                System.out.println("写入行:"+commitCount + " , 结束");
            }
            return new long[]{commitCount,0};
        }catch (Exception ex){
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
