package com.maywide.bi;

import com.alibaba.fastjson.JSON;
import com.maywide.bi.core.job.DeleteOldTableServices;
import com.maywide.bi.core.job.ScheduleManager;
import com.maywide.bi.util.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Component
public class ScheduleApplicationListener implements ApplicationListener {
    private static final Logger log = LoggerFactory.getLogger(ScheduleApplicationListener.class);
    @Autowired
    private ScheduleManager schedule;
    @Autowired
    private DeleteOldTableServices deleteOldTableServices;
    private static int count = 0;
    @Override
    public void onApplicationEvent(ApplicationEvent applicationEvent) {
        /*ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);
        executorService.scheduleWithFixedDelay(schedule,1000, 2000, TimeUnit.MILLISECONDS);*/
        // 第二个参数为首次执行的延时时间，第三个参数为定时执行的间隔时间
        if(count==0){
            ScheduleManager.service.scheduleAtFixedRate(schedule, 1000, 2000, TimeUnit.MILLISECONDS);
            System.out.println("核心数:---"+Runtime.getRuntime().availableProcessors());
            count ++;

            Date today = new Date();
            String timeStr  = " 12:51:00";
            Date runTime = DateUtils.parseDate(DateUtils.formatDate(today)+ timeStr,new SimpleDateFormat(DateUtils.DEFAULT_TIME_FORMAT));

            if(today.after(runTime)){
                //今天已经过了指定的运行时间，第二条的四点再执行
                runTime = DateUtils.addNday(runTime,1);
            }
            long initialDeplay = runTime.getTime() - today.getTime();
            log.info("删除临时表的开始时间为 "+ runTime+",并且每天 " + timeStr + ",执行一次");
            Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(deleteOldTableServices,initialDeplay,24*60*60*1000,TimeUnit.MILLISECONDS);
        }

    }
}
