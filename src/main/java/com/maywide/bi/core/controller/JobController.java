package com.maywide.bi.core.controller;

import com.alibaba.druid.support.json.JSONUtils;
import com.maywide.bi.config.datasource.dynamic.Constants;
import com.maywide.bi.config.datasource.dynamic.DbContextHolder;
import com.maywide.bi.core.execute.Operation;
import com.maywide.bi.core.schedule.ScheduleJob;
import com.maywide.bi.core.services.JobServices;
import com.maywide.bi.util.DateUtils;
import com.maywide.bi.util.SpringJdbcTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@RestController
@RequestMapping("/job")
public class JobController {

    private static final Logger log = LoggerFactory.getLogger(JobController.class);
    @Autowired
    private JobServices jobServices;

    @Autowired
    private SpringJdbcTemplate springJdbcTemplate;

    @RequestMapping("/run/{id}")
    public String runJob(@PathVariable("id") int id, @RequestParam(name = "sday")String sday,@RequestParam("eday")String eday,@RequestParam("bethour")int bethour){
        Date startDay = DateUtils.parseDate(sday,new SimpleDateFormat("yyMMdd"));
        Date endDay = DateUtils.parseDate(eday,new SimpleDateFormat("yyMMdd"));

        if(endDay.before(startDay)){
            return "确保结束时间不小于开始时间";
        }
        System.out.println(id);
        System.out.println(startDay);
        System.out.println(endDay);
        int range = DateUtils.getDiscrepantDays(startDay,endDay) + 1;
        System.out.println("天数:" + range);

        for (int i = 0; i < range; i++) {
            //天
            Date startTime = DateUtils.parseTime(DateUtils.formatDate(startDay)+" 00:00:00");
            Date dayEndTime = DateUtils.parseTime(DateUtils.formatDate(startDay)+" 23:59:59");
            while (dayEndTime.after(startTime)){
                Date stime = startTime;
                Date etime = org.apache.commons.lang.time.DateUtils.addHours(startTime,bethour);
                System.out.println("执行id = "+ id+",开始时间="+stime+",结束时间"+etime);
                List<Date> list = Operation.getSEtimeListbyDay(stime,etime);
                if(null != list && !list.isEmpty()){
                    for (int j = 0; j < list.size(); j+=2) {
                        jobServices.runJobByIdAndDate(id,list.get(j),list.get(j+1));
                    }
                }
                startTime = etime ;
            }
            startDay = DateUtils.addNday(startDay,1);
        }
        return id + JSONUtils.toJSONString(startDay) +  JSONUtils.toJSONString(endDay);
    }

    @RequestMapping("/run/schedule/{scheduleid}")
    public String runSchedule(@PathVariable("scheduleid") int scheduleid, @RequestParam(name = "sday")String sday,@RequestParam("eday")String eday,@RequestParam("bethour")int bethour){
        Date startDay = DateUtils.parseDate(sday,new SimpleDateFormat("yyMMdd"));
        Date endDay = DateUtils.parseDate(eday,new SimpleDateFormat("yyMMdd"));

        if(endDay.before(startDay)){
            return "确保结束时间不小于开始时间";
        }
        System.out.println(scheduleid);
        System.out.println(startDay);
        System.out.println(endDay);
        int range = DateUtils.getDiscrepantDays(startDay,endDay) + 1;
        Date endTime = DateUtils.parseTime(DateUtils.formatDate(startDay)+" 00:00:00");
        log.info("天数:" + range);
        int count = 0;
        for (int i = 0; i < range; i++) {
            //天
            Date startTime = DateUtils.parseTime(DateUtils.formatDate(startDay)+" 00:00:00");
            Date secondDayTime = DateUtils.addNday(startTime,1);

            Date stime = startTime;
            while (secondDayTime.after(stime)){
                Date etime = org.apache.commons.lang.time.DateUtils.addHours(stime,bethour);
                if(etime.after(secondDayTime)){
                    etime = secondDayTime;
                }
                DbContextHolder.setDBType(Constants.DEFAULT_DATA_SOURCE_NAME);
                List<Date> list = Operation.getSEtimeListbyDay(stime,etime);
                if(null != list && !list.isEmpty()){
                    for (int j = 0; j < list.size(); j+=2) {

                        String sql = "select * from bi_schedule_job where `status` = ? and scheduleid = ? order by sort asc";
                        List<ScheduleJob> sjobList = springJdbcTemplate.query(sql,new Object[]{Constants.SCHEDULE_JOB.STATUS_NORMAL,scheduleid},ScheduleJob.rowMapper());
                        if(null != sjobList && !sjobList.isEmpty()){
                            for (ScheduleJob scheduleJob : sjobList) {
                                log.info("执行scheuldid  = "+ scheduleid +"jobID ="+scheduleJob.getJobid()+",开始时间="+list.get(j)+",结束时间"+list.get(j+1));
                                count ++;
                                jobServices.runJobByIdAndDate((int) scheduleJob.getJobid(),list.get(j),list.get(j+1));
                            }
                        }
                    }
                }
                stime = etime ;
            }
            startDay = DateUtils.addNday(startDay,1);
        }
        return "scheduleid= "+ scheduleid +",合计执行 " + count +"次,请查看日志，等待执行完成";
    }

}
