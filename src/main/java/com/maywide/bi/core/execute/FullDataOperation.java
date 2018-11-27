package com.maywide.bi.core.execute;

import com.maywide.bi.util.DateUtils;
import com.maywide.bi.util.SqlUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.tomcat.util.threads.ThreadPoolExecutor;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

@Component
public class FullDataOperation extends Operation {
    public static ExecutorService fullExcutor = Executors.newSingleThreadScheduledExecutor();
   /* @Override
    public void execute(String sourceDataSource,String sourceSql,String targetDataSource, String targetTable) {
        fullExcutor.execute(new Worker( sourceDataSource, sourceSql, targetDataSource,  targetTable));
    }*/

    @Override
    public void execute(String sourceDataSource, String sourceSql, String targetDataSource, String targetTable, Date startTime,Date endTime) {
        logger.info("全量数据，不根据时间为依据，执行全量备份");
        fullExcutor.execute(new Worker( sourceDataSource, sourceSql, targetDataSource,  targetTable,startTime));
    }

    // 线程内部类，Thread或者Runnable均可
    private class Worker extends Thread {
        private Worker(String sourceDataSource,String sourceSql,String targetDataSource, String targetTable){
            this.sourceDataSource = sourceDataSource;
            this.sourceSql = sourceSql;
            this.targetDataSource = targetDataSource;
            this.targetTable = targetTable;
            /**默认生成前一天的数据*/
            targetDate = DateUtils.getYesterday();
        }
        private Worker(String sourceDataSource,String sourceSql,String targetDataSource, String targetTable,Date targetDate){
            this(sourceDataSource, sourceSql, targetDataSource,  targetTable);
            this.targetDate = targetDate;
        }
        private Date targetDate;
        protected String targetDataSource;
        protected String targetTable;
        protected String sourceDataSource;
        protected String sourceSql;
        @Override
        public void run() {
            logger.info("【全量数据】-线程"+currentThread().getName() +"启动");
            logger.info(executor.toString());
            logger.info("更改前的sql = " + sourceSql);
            sourceSql = SqlUtil.sqlAddTwoDate(sourceSql,targetDate);
            logger.info("更改后的sql = " + sourceSql);

            jdbcUtilServices.backUpTable(targetDataSource,targetTable,targetDate);
            jdbcUtilServices.clearTable(targetDataSource,targetTable);

            int count = jdbcUtilServices.count(sourceDataSource,sourceSql);
            logger.info("统计出 数据库【"+sourceDataSource+"】的数据总数【"+count+"】,即将插入目标数据库【"+targetDataSource+"】的表【"+targetTable+"】");
            int pageSize = Operation.BATCH_PAGESIZE;
            logger.info("分页处理数据，每次最多取" + pageSize +"条数据");
            if(count > pageSize){
                int totalPageNum = (count  +  pageSize  - 1) / pageSize;
                for (int i = 0; i < totalPageNum; i++) {
                    logger.info("数据="+(i+1)*pageSize);
                    String sql = sourceSql +" limit " + i * pageSize + "," + 1 * pageSize;
                    List<Map<String,Object>> valueList = jdbcUtilServices.getListBySql(sourceDataSource,sql);
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    jdbcUtilServices.batchInsert(targetDataSource,targetTable,valueList).toString();
                }
            }else {
                List<Map<String,Object>> valueList = jdbcUtilServices.getListBySql(sourceDataSource,sourceSql);
                logger.info(jdbcUtilServices.batchInsert(targetDataSource,targetTable,valueList).toString());
            }
        }
    }
}
