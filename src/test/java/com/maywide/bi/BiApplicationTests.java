package com.maywide.bi;

import com.alibaba.druid.support.json.JSONUtils;
import com.alibaba.fastjson.JSON;
import com.maywide.bi.config.datasource.dynamic.Constants;
import com.maywide.bi.config.datasource.dynamic.DbContextHolder;
import com.maywide.bi.core.execute.Operation;
import com.maywide.bi.core.schedule.Schedule;
import com.maywide.bi.core.schedule.ScheduleJob;
import com.maywide.bi.core.services.JobServices;
import com.maywide.bi.util.DateUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

/*@RunWith(SpringRunner.class)
@SpringBootTest*/
public class BiApplicationTests {

	@Autowired
	private JobServices jobServices;
	/*@Test
	public void contextLoads() {
		jobServices.excute();
		try {
			Thread.sleep(10000*1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}*/

	/*@Test
	public void Test(){
		ConcurrentLinkedQueue<String> scheduleQueue = new ConcurrentLinkedQueue<>();
		List<String> list = new ArrayList<>();
		list.add("123");
		list.add("12333");
		list.add("44");
		scheduleQueue.addAll(list);

		System.out.println(scheduleQueue.poll());
		System.out.println(scheduleQueue.size());
		System.out.println(scheduleQueue.poll());
		System.out.println(scheduleQueue.size());
	}*/
	@Test
	public void Test(){
		Date stime = DateUtils.parseTime("2018-09-11 00:00:00");
		Date etime = DateUtils.parseTime("2018-09-13 01:00:00");
        List<Date> list = Operation.getSEtimeListbyDay(stime,etime);
        System.out.println(JSONUtils.toJSONString(list));
	}


	@Test
	public void testBetTime(){
		String sday = "20181001";
		String eday = "20181002";
		int bethour= 12;
		Date startDay = DateUtils.parseDate(sday,new SimpleDateFormat("yyMMdd"));
		Date endDay = DateUtils.parseDate(eday,new SimpleDateFormat("yyMMdd"));

		/*if(endDay.before(startDay)){
			return "确保结束时间不小于开始时间";
		}*/

		System.out.println(startDay);
		System.out.println(endDay);
		int range = DateUtils.getDiscrepantDays(startDay,endDay) + 1;
		Date endTime = DateUtils.parseTime(DateUtils.formatDate(startDay)+" 00:00:00");
		System.out.println("天数:" + range);
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
						System.out.println("开始时间="+list.get(j)+",结束时间"+list.get(j+1));
					}
				}
				stime = etime ;
			}
			startDay = DateUtils.addNday(startDay,1);
		}

	}



	@Test
	public void tt (){
			Date startTime = DateUtils.parseTime("2018-11-27 23:00:00");
			Date endTime= DateUtils.parseTime("2018-11-28 00:00:00");
			List<Date> dateList = new ArrayList();
			if (!DateUtils.isSameDay(startTime, endTime))
			{
				int range = DateUtils.getDiscrepantDays(startTime, endTime) + 2;
				Date sTime = null;
				Date eTime = null;
				for (int i = 0; i < range; i++)
				{
					if (i == 0) {
						sTime = startTime;
					} else {
						sTime = eTime;
					}
					if (i == range - 1) {
						eTime = endTime;
					} else {
						eTime = DateUtils.parseTime(DateUtils.formatDate(DateUtils.addNday(sTime, 1)) + " 00:00:00");
					}
					if (eTime.after(sTime))
					{
						dateList.add(sTime);
						dateList.add(eTime);
					}
				}
			}
			else
			{
				dateList.add(startTime);
				dateList.add(endTime);
			}
		for (java.util.Date date : dateList) {
			System.out.println(date);
		}
		System.out.println("本地");
		startTime = DateUtils.parseTime("2018-11-27 23:00:00");
		endTime= DateUtils.parseTime("2018-11-28 00:00:00");
		dateList = Operation.getSEtimeListbyDay(startTime,endTime);
		for (java.util.Date date : dateList) {
			System.out.println(date);
		}
		}


}
