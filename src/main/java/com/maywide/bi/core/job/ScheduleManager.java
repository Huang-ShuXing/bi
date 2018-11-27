package com.maywide.bi.core.job;

import com.alibaba.fastjson.JSON;
import com.maywide.bi.config.datasource.dynamic.Constants;
import com.maywide.bi.config.datasource.dynamic.DbContextHolder;
import com.maywide.bi.core.schedule.Schedule;
import com.maywide.bi.core.schedule.ScheduleJob;
import com.maywide.bi.core.services.JobServices;
import com.maywide.bi.util.SpringJdbcTemplate;
import org.apache.tomcat.util.threads.ThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;

@Component
public class ScheduleManager implements Runnable {
    public static ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

    public static ThreadPoolExecutor schedulePool = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 2 + 1, Runtime.getRuntime().availableProcessors() * 3 + 1, 30, TimeUnit.SECONDS,
            new LinkedBlockingDeque<>(),new ThreadPoolExecutor.CallerRunsPolicy());

    private static final ConcurrentLinkedQueue<Schedule> scheduleQueue = new ConcurrentLinkedQueue<>();
    private static final Set<Schedule> scheduleSet = Collections.synchronizedSet(new HashSet<>());

    private Logger logger = LoggerFactory.getLogger(ScheduleManager.class);
    @Autowired
    private SpringJdbcTemplate springJdbcTemplate;

    @Autowired
    private JobServices jobServices;

    private List<Schedule> scheduleList ;


    @Override
    public void run() {
        //1.查询 全部要执行的任务
        //2.添加到执行的队列中
        //3.更新数据库的下一次执行时间
        //4.多线程开始执行
        this.getScheduleList().addScheduleToQueue().updateScheduleTime().start();
    }

    /***
     * 获取全部要执行的计划
     * @return
     */
    private ScheduleManager getScheduleList(){
        String sql = "select * from bi_schedule where `status` = ? and next_time <= ? ";
        scheduleList = springJdbcTemplate.query(sql,new Object[]{Constants.SCHEDULE.STATUS_NORMAL,new Date()},Schedule.rowMapper());
        return this;
        /*select * from bi_schedule where `status` = 1 and next_time > '2018-08-06 14:17:00';
        update bi_schedule set next_time = date_add(next_time, interval space_time second) where id =  1*/
    }


    private ScheduleManager addScheduleToQueue(){
        if(null == scheduleList || scheduleList.isEmpty()){
            return this;
        }
        ScheduleManager.scheduleQueue.addAll(scheduleList);
        return this;
    }


    /***
     * 更新Schedule
     */
    private ScheduleManager updateScheduleTime(){
        if(null == this.scheduleList || this.scheduleList.isEmpty()){
            return this;
        }
        List<Integer> idList = new ArrayList<>(scheduleList.size());
        for (Schedule schedule : scheduleList) {
            idList.add((int) schedule.getId());
        }

        try{
            DbContextHolder.setDBType(Constants.DEFAULT_DATA_SOURCE_NAME);
            String updateSql = "update bi_schedule set status = '"+Constants.SCHEDULE.STATUS_RUNNING+"', last_time = next_time , next_time = date_add(next_time, interval space_time second) where id in (:idList)";
            NamedParameterJdbcTemplate namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(springJdbcTemplate);
            Map<String, Object> params = new HashMap<String, Object>();
            params.put("idList",idList);
            int x =namedParameterJdbcTemplate.update(updateSql,params);
            logger.info("finishJob 结果:"+ x);
        }catch (Exception e){
            e.printStackTrace();
        }
        return this;
    }

    /***
     * 执行
     * @return
     */
    private ScheduleManager start(){
        int size = ScheduleManager.scheduleQueue.size();
        if(size > 0){
            for (int i = 0; i < size; i++) {
                Schedule schedule = ScheduleManager.scheduleQueue.poll();
                if(ScheduleManager.scheduleSet.contains(schedule)){
                    logger.info("当前任务:"+ JSON.toJSONString(schedule)+",已经在执行，重新入一次队列");
                    ScheduleManager.scheduleQueue.add(schedule);
                }else {
                    //执行
                    ScheduleManager.scheduleSet.add(schedule);
                    schedulePool.execute(new ScheduleServices(schedule));
                }
            }
        }
        return this;
    }


    private class ScheduleServices implements Runnable{
        public ScheduleServices(Schedule schedule) {
            this.schedule = schedule;
        }

        private Schedule schedule;
        @Override
        public void run() {
            try {
                this.startScheduel();
            }catch (Exception e){
                logger.error("【！！！任务出错】 "+schedule.getName() +"执行出错!!!请检查数据");
                logger.error(e.getMessage(),e);
            }finally {
                this.finish();
            }
        }
        public void startScheduel(){
            DbContextHolder.setDBType(Constants.DEFAULT_DATA_SOURCE_NAME);
            String sql = "select * from bi_schedule_job where `status` = ? and scheduleid = ? order by sort asc";
            List<ScheduleJob> sjobList = springJdbcTemplate.query(sql,new Object[]{Constants.SCHEDULE_JOB.STATUS_NORMAL,schedule.getId()},ScheduleJob.rowMapper());
            if(null != sjobList && !sjobList.isEmpty()){
                for (ScheduleJob scheduleJob : sjobList) {
                    logger.info("【任务即将执行】ID = " + scheduleJob.getJobid() +"");
                    this.startOneJob(scheduleJob.getJobid(),schedule.getLastTime(),schedule.getNextTime());
                    logger.info("【任务执行完成】ID = " + scheduleJob.getJobid() +"");
                }
            }
        }
        /***
         * 执行任务
         * @param jobid
         * @return
         */
        private boolean startOneJob(long jobid,Date startTime ,Date endTime)  {
            jobServices.runJobByIdAndDate((int) jobid,startTime,endTime);
            return true;
        }

        private void finish(){
            DbContextHolder.setDBType(Constants.DEFAULT_DATA_SOURCE_NAME);
            String changeStatus = " update bi_schedule set status = '"+Constants.SCHEDULE.STATUS_NORMAL+"' where id =  "+schedule.getId();
            springJdbcTemplate.execute(changeStatus);
            ScheduleManager.scheduleSet.remove(schedule);
        }
    }

}
