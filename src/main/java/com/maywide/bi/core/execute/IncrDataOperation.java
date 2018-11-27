package com.maywide.bi.core.execute;

import com.maywide.bi.core.job.DeleteOldTableServices;
import com.maywide.bi.core.services.JdbcUtilServices;
import com.maywide.bi.core.services.TableUtil;
import com.maywide.bi.util.SqlUtil;
import org.apache.commons.lang.StringUtils;
import com.maywide.bi.util.DateUtils;
import org.apache.tomcat.util.threads.ThreadPoolExecutor;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

@Component
public class IncrDataOperation  extends  Operation{
    public static int WAIT_TIME = 160;
    public static ThreadPoolExecutor incrPoolExecutor = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 2 + 1, Runtime.getRuntime().availableProcessors() * 3 + 1, 30, TimeUnit.SECONDS,
            new LinkedBlockingDeque<>(Operation.QUE_SIZE),new ThreadPoolExecutor.CallerRunsPolicy());
    /*@Override
    public  void execute(String sourceDataSource,String sourceSql,String targetDataSource, String targetTable){
        logger.error("不会执行该方法");
        //executor.execute(new Worker( sourceDataSource, sourceSql, targetDataSource,  targetTable));
    }*/
    /***
     * 执行日期执行 增量数据
     * @param sourceDataSource
     * @param sourceSql
     * @param targetDataSource
     * @param targetTable
     * @param startTime
     * @param endTime
     */
    @Override
    public  void execute(String sourceDataSource,String sourceSql,String targetDataSource, String targetTable,Date startTime,Date endTime){
        // executor.execute(new Worker( sourceDataSource, sourceSql, targetDataSource,  targetTable,startTime,endTime));
        List<Date> times = Operation.getSEtimeListbyDay(startTime,endTime);
        if (times != null && !times.isEmpty()) {
            for (int i = 0; i < times.size(); i+=2) {
                this.workRun( sourceDataSource, sourceSql, targetDataSource,  targetTable,times.get(i),times.get(i+1));
            }
        }else {
            logger.error("时间转换时出现问题,startTime = " + startTime+",endTime = "+endTime);
        }
        //this.workRun( sourceDataSource, sourceSql, targetDataSource,  targetTable,startTime,endTime);
    }
    private void workRun(String sourceDataSource,String sourceSql,String targetDataSource, String targetTable,Date startTime,Date endTime){
        logger.info("【增量数据】-线程"+Thread.currentThread().getName() +"启动"+",源数据库为="+sourceDataSource);
        logger.info(executor.toString());

        //对目标表增加一个日数据表
        if(TableUtil.needBack(targetTable) && !jdbcUtilServices.existTable(targetDataSource, TableUtil.getDayTableName(targetTable,startTime))){
            /*logger.info("开始为数据库【"+targetDataSource+"】新建按日生成表【"+TableUtil.getDayTableName(targetTable,targetDate)+"】");*/
            jdbcUtilServices.copyTableNoData(targetDataSource,targetTable,TableUtil.getDayTableName(targetTable,startTime));
            /*logger.info("数据库【"+targetDataSource+"】,生成表【"+TableUtil.getDayTableName(targetTable,targetDate)+"】成功");*/
        }
        logger.info("更改前的sql = " + sourceSql);
        sourceSql = SqlUtil.sqlAddSETime(SqlUtil.sqlDayTransform(sourceSql,startTime),startTime,endTime);
//        sourceSql = SqlUtil.sqlAddSETime(sourceSql,startTime,endTime);
        logger.info("更改后的sql = " + sourceSql);

        int count = jdbcUtilServices.count(sourceDataSource,sourceSql);
        logger.info("统计出 数据库【"+sourceDataSource+"】的数据总数【"+count+"】,即将插入目标数据库【"+targetDataSource+"】的表【"+targetTable+"】");
        int pageSize =Operation.BATCH_PAGESIZE;
        CountDownLatch cdLatch = null;
        if(count > pageSize){
            int totalPageNum = (count  +  pageSize  - 1) / pageSize;
            cdLatch = new CountDownLatch(totalPageNum);
            for (int i = 0; i < totalPageNum; i++) {
                logger.info("数据="+(i+1)*pageSize);
                String sql = sourceSql +" limit " + i * pageSize + "," + 1 * pageSize;
                List<Map<String,Object>> valueList = jdbcUtilServices.getListBySql(sourceDataSource,sql);
                try {
                    Thread.sleep(WAIT_TIME);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage(),e);
                }
                IncrWork incrWork = new IncrWork(targetDataSource,targetTable,sourceDataSource,startTime,endTime,valueList);
                incrWork.setCountDownLatch(cdLatch);
                incrPoolExecutor.execute(incrWork);
                //jdbcUtilServices.batchInsert(targetDataSource,targetTable,valueList);
            }
        }else {
            cdLatch = new CountDownLatch(1);
            List<Map<String,Object>> valueList = jdbcUtilServices.getListBySql(sourceDataSource,sourceSql);
            IncrWork incrWork = new IncrWork(targetDataSource,targetTable,sourceDataSource,startTime,endTime,valueList);
            incrWork.setCountDownLatch(cdLatch);
            incrPoolExecutor.execute(incrWork);
            //jdbcUtilServices.batchInsert(targetDataSource,targetTable,valueList);
        }

        try {
            //主线程等待
            logger.info("主线程等待线程池的线程[线程池数 = "+cdLatch.getCount()+"]执行完....");
            if(null != cdLatch){
                cdLatch.await();
            }
            logger.info("线程池执行完成....");
        } catch (InterruptedException e) {
            logger.error(e.getMessage(),e);
        }
        logger.info("【增量数据-完成并退出】-线程"+Thread.currentThread().getName() +"启动"+",源数据库【"+targetDataSource+"】,"+"表【"+targetTable+"】,时间["+DateUtils.formatTime(startTime)+"-"+DateUtils.formatTime(endTime)+"]");

    }
    private class IncrWork implements  Runnable{
        private CountDownLatch countDownLatch;

        public void setCountDownLatch(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        private String targetDataSource;
        private String targetTable;
        private String sourceDataSource;
        private Date startTime;
        private Date endTime;
        List<Map<String,Object>> valueList;

        public IncrWork(String targetDataSource, String targetTable, String sourceDataSource, Date startTime, Date endTime, List<Map<String, Object>> valueList) {
            this.targetDataSource = targetDataSource;
            this.targetTable = targetTable;
            this.sourceDataSource = sourceDataSource;
            this.startTime = startTime;
            this.endTime = endTime;
            this.valueList = valueList;
        }

        @Override
        public void run() {
            logger.info("【增量数据】-【INCR -WORK 】线程"+Thread.currentThread().getName() +"启动"+"sourceDatasource="+sourceDataSource);
            logger.info("incrPoolExecutor 线程池"+incrPoolExecutor.toString());
            HashMap<String ,Object > inParam = new HashMap<>();
            inParam.put("intime", endTime);
            try {
                valueList = jdbcUtilServices.addOtherFiled(targetDataSource,targetTable,sourceDataSource,startTime,valueList,inParam);//日期以开始时间为准
                //插入目标表的同时，插入当如备份表
                if(TableUtil.needBack(targetTable)) {
                    //增加一个扫描的删除日表的
                    DeleteOldTableServices.addDelTableSet(targetTable);
                    jdbcUtilServices.batchInsertAndBack( targetDataSource, targetTable, valueList, startTime);
                }else {
                    jdbcUtilServices.batchInsert(targetDataSource, targetTable, valueList);
                }
            }catch (Exception e ){
                logger.info("【增量数据-执行出错】-【INCR -WORK 】线程"+Thread.currentThread().getName() +"启动"+"sourceDatasource="+sourceDataSource);
                logger.error(e.getMessage(),e);
            }finally {
                if(null != countDownLatch){
                    countDownLatch.countDown();
                }
            }
        }
    }



    /**
     * 线程内部类，Thread或者Runnable均可
     */
    /*private class Worker extends Thread {


       *//* private Worker(String sourceDataSource,String sourceSql,String targetDataSource, String targetTable){
            this.sourceDataSource = sourceDataSource;
            this.sourceSql = sourceSql;
            this.targetDataSource = targetDataSource;
            this.targetTable = targetTable;
            targetDate = DateUtils.getYesterday();//默认生成昨天的数据
        }*//*

        private Worker(String sourceDataSource,String sourceSql,String targetDataSource, String targetTable,Date startTime,Date endTime){
            this.sourceDataSource = sourceDataSource;
            this.sourceSql = sourceSql;
            this.targetDataSource = targetDataSource;
            this.targetTable = targetTable;
            this.startTime = startTime;
            this.endTime = endTime;
        }

        private String targetDataSource;
        private String targetTable;
        private String sourceDataSource;
        private String sourceSql;


        private Date startTime;
        private Date endTime;



        @Override
        public void run() {
            logger.info("【增量数据】-线程"+currentThread().getName() +"启动"+",源数据库为="+sourceDataSource);
            logger.info(executor.toString());

            //TODO  对目标表增加一个昨日数据表
            if(TableUtil.needBack(targetTable) && !jdbcUtilServices.existTable(targetDataSource, TableUtil.getDayTableName(targetTable,startTime))){
                *//*logger.info("开始为数据库【"+targetDataSource+"】新建按日生成表【"+TableUtil.getDayTableName(targetTable,targetDate)+"】");*//*
                jdbcUtilServices.copyTableNoData(targetDataSource,targetTable,TableUtil.getDayTableName(targetTable,startTime));
                *//*logger.info("数据库【"+targetDataSource+"】,生成表【"+TableUtil.getDayTableName(targetTable,targetDate)+"】成功");*//*
            }
            logger.info("更改前的sql = " + sourceSql);
            sourceSql = SqlUtil.sqlAddSETime(sourceSql,startTime,endTime);
            logger.info("更改后的sql = " + sourceSql);

            int count = jdbcUtilServices.count(sourceDataSource,sourceSql);
            logger.info("统计出 数据库【"+sourceDataSource+"】的数据总数【"+count+"】,即将插入目标数据库【"+targetDataSource+"】的表【"+targetTable+"】");
            int pageSize =Operation.BATCH_PAGESIZE;
            if(count > pageSize){
                int totalPageNum = (count  +  pageSize  - 1) / pageSize;
                for (int i = 0; i < totalPageNum; i++) {
                    logger.info("数据="+(i+1)*pageSize);
                    String sql = sourceSql +" limit " + i * pageSize + "," + 1 * pageSize;
                    List<Map<String,Object>> valueList = jdbcUtilServices.getListBySql(sourceDataSource,sql);
                    try {
                        Thread.sleep(WAIT_TIME);
                    } catch (InterruptedException e) {
                        logger.error(e.getMessage(),e);
                    }
                    incrPoolExecutor.execute(new IncrWork(valueList));
                    //jdbcUtilServices.batchInsert(targetDataSource,targetTable,valueList);
                }
            }else {
                List<Map<String,Object>> valueList = jdbcUtilServices.getListBySql(sourceDataSource,sourceSql);
                incrPoolExecutor.execute(new IncrWork(valueList));
                //jdbcUtilServices.batchInsert(targetDataSource,targetTable,valueList);
            }

        }
        private class IncrWork implements  Runnable{
            List<Map<String,Object>> valueList;
            private IncrWork(List<Map<String,Object>> valueList){
                this.valueList = valueList;
            }
            @Override
            public void run() {
                logger.info("【增量数据】-【INCR -WORK 】线程"+Thread.currentThread().getName() +"启动"+"sourceDatasource="+sourceDataSource);
                logger.info("incrPoolExecutor 线程池"+incrPoolExecutor.toString());
                HashMap<String ,Object > inParam = new HashMap<>();
                inParam.put("intime", startTime);
                valueList = SqlUtil.addOtherFiled(sourceDataSource,targetTable,startTime,valueList,inParam);//日期以开始时间为准
                //插入目标表的同时，插入当如备份表
                if(TableUtil.needBack(targetTable)) {
                    jdbcUtilServices.batchInsertAndBack( targetDataSource, targetTable, valueList, startTime);
                }else {
                    jdbcUtilServices.batchInsert(targetDataSource, targetTable, valueList);
                }
            }
        }
    }*/
}
