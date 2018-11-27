package com.maywide.bi.core.execute;

import com.maywide.bi.core.services.JdbcUtilServices;
import com.maywide.bi.util.DateUtils;
import org.apache.tomcat.util.threads.ThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public abstract class Operation{
    protected static int QUE_SIZE = 200;
    protected static int BATCH_PAGESIZE = 1000;
    protected static ThreadPoolExecutor executor = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors()*2+1,Runtime.getRuntime().availableProcessors()*3+1,30, TimeUnit.SECONDS,
            new LinkedBlockingDeque<>(Operation.QUE_SIZE));
    protected static Logger logger = LoggerFactory.getLogger(Operation.class);
    @Autowired
    protected JdbcUtilServices jdbcUtilServices;

    /*public abstract void execute(String sourceDataSource,String sourceSql,String targetDataSource, String targetTable);*/

    public abstract void execute(String sourceDataSource,String sourceSql,String targetDataSource, String targetTable,Date startTime,Date endTime);


    /***
     * 根据开始时间和结束时间，算出以天为单位切割的时间段数组，用来适应按天统计的数组
     * 如startTime = 10号23:30:00 ,endTime = 12号00::30:00<br/>
     * 则结果要为 <br/>
     * 10号23:30:00 -11号00:00:00<br/>
     * 11号00:00:00 -12号00:00:00<br/>
     * 12号00:00:00 -13号00:30:00<br/>
     * 列表长度必须为偶数,etime>stime才满足
     * @param startTime
     * @param endTime
     * @return
     */
    public static List<Date> getSEtimeListbyDay(Date startTime,Date endTime){
        List<Date> dateList = new ArrayList<>();
        if(!DateUtils.isSameDay(startTime,endTime)){
            int range = DateUtils.getDiscrepantDays(startTime,endTime) + 1;
            Date sTime = null;
            Date eTime = null;
            for (int i = 0; i < range; i++) {
                if(i == 0){
                    sTime = startTime;
                }else {
                    sTime = eTime;
                }
                if(i == range -1){
                    eTime = endTime;
                }else {
                    eTime = DateUtils.parseTime(DateUtils.formatDate(DateUtils.addNday(sTime,1))+" 00:00:00");
                }
                if(eTime.after(sTime)){
                    dateList.add(sTime);
                    dateList.add(eTime);
                }

            }
        }else {
            dateList.add(startTime);
            dateList.add(endTime);
        }
        return dateList;
    }
}
