package com.maywide.bi.core.execute;

import ch.qos.logback.core.db.dialect.DBUtil;
import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.fastjson.JSON;
import com.maywide.bi.core.services.JdbcUtilServices;
import com.maywide.bi.core.services.TableUtil;
import com.maywide.bi.util.DateUtils;
import com.maywide.bi.util.SqlUtil;
import org.apache.commons.lang.StringUtils;
import org.aspectj.apache.bcel.classfile.Constant;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import java.util.*;
import java.util.concurrent.CountDownLatch;

/****
 * 生成指标操作（way = 2)
 * 1.获取指标SQL,源数据库,目标数据库，目标数据表
 * 2.解析SQL （如果sql 中包含 '[dynamic_day]' ,将 该字符串转为当天日期 '_yyymmdd' ,否则SQL 不转）
 * 3.查询目标库，目标数据表 存在与否，不存在则报错
 * 4.根据解析后的SQL 去源数据库查询数据（先查询大小，再查具体数据）
 * 5.根据数据大小，多线程分页批量插入目标库表
 */
@Component
public class GenerateIndexOperation extends Operation {

    /*@Override
    public void execute(String sourceDataSource, String sourceSql, String targetDataSource, String targetTable) {
        logger.error("不要调用这个方法了，没用");
     //   executor.execute(new Worker( sourceDataSource, sourceSql, targetDataSource,  targetTable));
    }*/

    /***
     * 指定日期执行 增量生成指标
     * @param sourceDataSource
     * @param sourceSql
     * @param targetDataSource
     * @param targetTable
     * @param startTime
     * @param endTime
     */
    @Override
    public  void execute(String sourceDataSource,String sourceSql,String targetDataSource, String targetTable,Date startTime,Date endTime){
        //executor.execute(new Worker( sourceDataSource, sourceSql, targetDataSource,  targetTable,startTime,endTime));
        List<Date> times = Operation.getSEtimeListbyDay(startTime,endTime);
        if (times != null && !times.isEmpty()) {
            for (int i = 0; i < times.size(); i+=2) {
                this.workRun(targetDataSource,targetTable,sourceDataSource,sourceSql,times.get(i),times.get(i+1));
            }
        }else {
            logger.error("时间转换时出现问题,startTime = " + startTime+",endTime = "+endTime);
        }

    }
    private void workRun(String targetDataSource,String targetTable, String sourceDataSource,String sourceSql,Date startTime,Date endTime){
        logger.info("【指标生成】-线程"+Thread.currentThread().getName() +"启动"+",源数据库【"+targetDataSource+"】,"+"表【"+targetTable+"】,时间["+DateUtils.formatTime(startTime)+"-"+DateUtils.formatTime(endTime)+"]");
        Assert.notNull(sourceDataSource,"[源数据库]不能为空");
        Assert.notNull(sourceSql,"[源数据表]不能为空");
        Assert.notNull(targetDataSource,"[目标数据库]不能为空");
        Assert.notNull(targetTable,"[目标表]不能为空");

        logger.info("[解析SQL] - 前 :" + sourceSql);
        sourceSql = SqlUtil.sqlAddSETime(SqlUtil.sqlDayTransform(sourceSql,startTime),startTime,endTime);
        logger.info("[解析SQL] - 后 :" + sourceSql);

        if(!jdbcUtilServices.existTable(targetDataSource,targetTable)){
            logger.error("[生成指标线程]...发现 数据库【"+targetDataSource+"】,"+"表【"+targetTable+"】不存在!!!");
            logger.error("【指标生成】 程序退出");
            return ;
        }

        logger.info("【开始生成指标】 - 数据库 ["+targetDataSource+"] ,表 ["+targetTable+"] ,时间["+DateUtils.formatTime(startTime)+"-"+DateUtils.formatTime(endTime)+"]");
        int count = jdbcUtilServices.count(sourceDataSource,sourceSql);
        logger.info("【指标数据】- 总共 :" + count);

        //删除旧指标数据并更新
        if(jdbcUtilServices.tableHasColumn(targetDataSource,targetTable,SqlUtil.FIELD_INDEXDAY)){
            String day = DateUtils.formatDate(startTime);
            String delSql = "delete from " + targetTable +" where " + SqlUtil.FIELD_INDEXDAY + "='"+day+"'";
            jdbcUtilServices.execute(targetDataSource,delSql);
        }

        CountDownLatch cdlatch = null;
        int pageSize = Operation.BATCH_PAGESIZE;
        if(count > pageSize){
            int totalPageNum = (count  +  pageSize  - 1) / pageSize;
            cdlatch = new CountDownLatch(totalPageNum);
            for (int i = 0; i < totalPageNum; i++) {
                logger.debug("数据="+(i+1)*pageSize);
                String sql = sourceSql +" limit " + i * pageSize + "," + 1 * pageSize;
                List<Map<String,Object>> valueList = jdbcUtilServices.getListBySql(sourceDataSource,sql);
                GenerateIndexWork indexWork =new GenerateIndexWork(targetDataSource,targetTable,sourceDataSource,startTime,endTime,valueList);
                indexWork.setCountDownLatch(cdlatch);
                executor.execute(indexWork);
            }
        }else {
            cdlatch = new CountDownLatch(1);
            List<Map<String,Object>> valueList = jdbcUtilServices.getListBySql(sourceDataSource,sourceSql);
            GenerateIndexWork indexWork =new GenerateIndexWork(targetDataSource,targetTable,sourceDataSource,startTime,endTime,valueList);
            indexWork.setCountDownLatch(cdlatch);
            executor.execute(indexWork);
        }

        try {
            //主线程等待
            logger.info("主线程等待线程池的线程[线程池数 = "+cdlatch.getCount()+"]执行完....");
            if(null != cdlatch){
                cdlatch.await();
            }
            logger.info("线程池执行完成....");
        } catch (InterruptedException e) {
            logger.error(e.getMessage(),e);
        }
        logger.info("【指标生成-完成并退出】-线程"+Thread.currentThread().getName() +"启动"+",源数据库【"+targetDataSource+"】,"+"表【"+targetTable+"】,时间["+DateUtils.formatTime(startTime)+"-"+DateUtils.formatTime(endTime)+"]");


    }

    private class GenerateIndexWork implements  Runnable{
        private CountDownLatch countDownLatch ;
        protected String targetDataSource;
        protected String targetTable;
        protected String sourceDataSource;

        private Date startTime;
        private Date endTime;
        List<Map<String,Object>> valueList;

        public GenerateIndexWork(String targetDataSource, String targetTable, String sourceDataSource, Date startTime, Date endTime, List<Map<String, Object>> valueList) {
            this.targetDataSource = targetDataSource;
            this.targetTable = targetTable;
            this.sourceDataSource = sourceDataSource;
            this.startTime = startTime;
            this.endTime = endTime;
            this.valueList = valueList;
        }

        public void setCountDownLatch(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            logger.info("【指标生成】- 【GenerateIndex-WORK 】线程 数据库 ["+targetDataSource+"] ,表 ["+targetTable+"] ,时间["+DateUtils.formatTime(startTime)+"-"+DateUtils.formatTime(endTime)+"]");
            HashMap<String ,Object > inParam = new HashMap<>();
            inParam.put("intime", endTime);
            inParam.put("optime", new Date());
            try {
                valueList = jdbcUtilServices.addOtherFiled(targetDataSource,targetTable,sourceDataSource,startTime,valueList,inParam);
                saveGenerateIndex(targetDataSource,targetTable,valueList,startTime);
            }catch (Exception e){
                logger.error("【指标生成-报错】- 【GenerateIndex-WORK 】线程 数据库 ["+targetDataSource+"] ,表 ["+targetTable+"] ,时间["+DateUtils.formatTime(startTime)+"-"+DateUtils.formatTime(endTime)+"]");
                logger.error(e.getMessage(),e);
            }finally {
                if(this.countDownLatch != null){
                    this.countDownLatch.countDown();
                }
            }
        }
    }


    /*private class Worker extends Thread {
        private Worker(String sourceDataSource, String sourceSql, String targetDataSource, String targetTable) {
            this.sourceDataSource = sourceDataSource;
            this.sourceSql = sourceSql;
            this.targetDataSource = targetDataSource;
            this.targetTable = targetTable;
        }

        private Date startTime;
        private Date endTime;

        private Worker(String sourceDataSource,String sourceSql,String targetDataSource, String targetTable,Date startTime,Date endTime){
            this(sourceDataSource, sourceSql, targetDataSource,  targetTable);
            this.startTime = startTime;
            this.endTime = endTime;
        }

        protected String targetDataSource;
        protected String targetTable;
        protected String sourceDataSource;
        protected String sourceSql;


        @Override
        public void run() {
            logger.info("【指标生成】-线程"+currentThread().getName() +"启动"+",源数据库【"+targetDataSource+"】,"+"表【"+targetTable+"】,时间["+DateUtils.formatTime(startTime)+"-"+DateUtils.formatTime(endTime)+"]");
            Assert.notNull(sourceDataSource,"[源数据库]不能为空");
            Assert.notNull(sourceSql,"[源数据表]不能为空");
            Assert.notNull(targetDataSource,"[目标数据库]不能为空");
            Assert.notNull(targetTable,"[目标表]不能为空");

            //IncrDataOperation.incrPoolExecutor.
            logger.info("[解析SQL] - 前 :" + sourceSql);
            sourceSql = SqlUtil.sqlDayTransform(sourceSql,startTime);
            logger.info("[解析SQL] - 后 :" + sourceSql);

            if(!jdbcUtilServices.existTable(targetDataSource,targetTable)){
                logger.error("[生成指标线程]...发现 数据库【"+targetDataSource+"】,"+"表【"+targetTable+"】不存在!!!");
                logger.error("【指标生成】 程序退出");
                return ;
            }
            //指标的生存,要等待增量的线程池执行完，确保数据一致
            int runCount = 0;
            while(runCount < 60){
                int c = IncrDataOperation.incrPoolExecutor.getActiveCount();
                if(c <= 0){
                    runCount ++;
                }else {
                    runCount = 0;
                }
                try {
                    Thread.sleep(IncrDataOperation.WAIT_TIME);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            logger.info("IncrDataOperation.incrPoolExecutor 的激活线程数 累计查询 60 次都为0 " );
            logger.info("【开始生成指标】 - 数据库 ["+targetDataSource+"] ,表 ["+targetTable+"] ,时间["+DateUtils.formatTime(startTime)+"-"+DateUtils.formatTime(endTime)+"]");
            int count = jdbcUtilServices.count(sourceDataSource,sourceSql);
            logger.info("【指标数据】- 总共 :" + count);
            //long t1 = System.currentTimeMillis();
            int pageSize = Operation.BATCH_PAGESIZE;
            if(count > pageSize){
                int totalPageNum = (count  +  pageSize  - 1) / pageSize;
                for (int i = 0; i < totalPageNum; i++) {
                    logger.debug("数据="+(i+1)*pageSize);
                    String sql = sourceSql +" limit " + i * pageSize + "," + 1 * pageSize;
                    List<Map<String,Object>> valueList = jdbcUtilServices.getListBySql(sourceDataSource,sql);
                    executor.execute(new GenerateIndexWork(valueList));
                }
            }else {
                List<Map<String,Object>> valueList = jdbcUtilServices.getListBySql(sourceDataSource,sourceSql);
                executor.execute(new GenerateIndexWork(valueList));
            }
            //long t2 = System.currentTimeMillis();
           // logger.info(" 【指标生成】 - ["+targetDataSource+"] ,表 ["+targetTable+"] ,日期["+DateUtils.formatDate(targetDate,DateUtils.FORMAT_YYMMDD)+"],耗时 [ " +(t2- t1)+ "]");
        }

        private class GenerateIndexWork implements  Runnable{
            List<Map<String,Object>> valueList;
            private GenerateIndexWork(List<Map<String,Object>> valueList){
                this.valueList = valueList;
            }
            @Override
            public void run() {
                logger.info("【指标生成】- 【GenerateIndex-WORK 】线程 数据库 ["+targetDataSource+"] ,表 ["+targetTable+"] ,时间["+DateUtils.formatTime(startTime)+"-"+DateUtils.formatTime(endTime)+"]");
                HashMap<String ,Object > inParam = new HashMap<>();
                inParam.put("intime", startTime);
                inParam.put("optime", new Date());
                valueList = SqlUtil.addOtherFiled(sourceDataSource,targetTable,startTime,valueList,inParam);
                saveGenerateIndex(targetDataSource,targetTable,valueList,startTime);
            }
        }
    }*/

    /***
     * 保存指标数据
     * @param targetDataSource
     * @param targetTable
     * @param valueList
     * @param startTime
     */
    @Transactional
    public void saveGenerateIndex(String targetDataSource,String targetTable,List<Map<String,Object>> valueList,Date startTime ){
        //先把当天已经生成的旧指标数据删除,并且判断是否

        if(jdbcUtilServices.tableHasColumn(targetDataSource,targetTable,SqlUtil.FIELD_INDEXDAY)){
            //是否有表动轨迹表，有则将记录备份
            String traceTable = targetTable +"_"+ SqlUtil.TRACE;
            if(jdbcUtilServices.existTable(targetDataSource,traceTable) ){
                jdbcUtilServices.batchInsert(targetDataSource,traceTable,valueList);
            }
        }
        jdbcUtilServices.batchInsert(targetDataSource,targetTable,valueList);
    }


    /***
     * 保存指标数据
     * @param targetDataSource
     * @param targetTable
     * @param valueList
     * @param startTime
     */
    /*@Transactional
    public void saveGenerateIndex(String targetDataSource,String targetTable,List<Map<String,Object>> valueList,Date startTime ){
        //先把当天已经生成的旧指标数据删除,并且判断是否
        String day = DateUtils.formatDate(startTime);
        if(SqlUtil.isHasIndexDay(targetTable)){
            //是否有表动轨迹表，有则将记录备份
            String traceTableDetail = targetTable +"_"+ SqlUtil.TRACE;
            if(jdbcUtilServices.existTable(targetDataSource,traceTableDetail) ){
                HashMap<String ,String > inParam = new HashMap<>();
                inParam.put("intime", "'"+DateUtils.formatTime(startTime)+"'");
                inParam.put("optime", "'"+DateUtils.formatTime(new Date())+"'");
                HashMap<String,String> whereParam = new HashMap<>();
                whereParam.put(SqlUtil.FIELD_INDEXDAY,"'"+day+"'");
                String detailDataSql = null;
                if(StringUtils.isNotEmpty(detailDataSql = jdbcUtilServices.createSaveTraceLogSql(targetDataSource, targetTable,inParam,whereParam))){
                    logger.info("detail 表 SQL = " + JSON.toJSONString(detailDataSql));
                    jdbcUtilServices.execute(targetDataSource,detailDataSql);
                }
            }
            //删除旧指标数据并更新
            String delSql = "delete from " + targetTable +" where " + SqlUtil.FIELD_INDEXDAY + "='"+day+"'";
            jdbcUtilServices.execute(targetDataSource,delSql);
        }
        jdbcUtilServices.batchInsert(targetDataSource,targetTable,valueList);
    }*/
}
