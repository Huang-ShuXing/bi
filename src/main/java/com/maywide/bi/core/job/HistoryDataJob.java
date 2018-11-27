package com.maywide.bi.core.job;

import javafx.concurrent.Worker;

import java.util.List;
import java.util.Map;

public class HistoryDataJob extends Job {
    /***
     * 查询任务信息，执行
     * @param jobid
     * @return
     */
    @Override
    public Object excuteJob(int jobid) {
        return new HistoryWork(jobid);
    }

    /***
     * 查询任务的逻辑，并且执行
     */
    private class HistoryWork implements Runnable{
        private int jobid ;
        private HistoryWork(int jobid){
            this.jobid = jobid;
        }

        @Override
        public void run() {
            String jobSql = "select * from bi_job_info where id = ? ";
            List<Map<String,Object>> list =springJdbcTemplate.queryForList(jobSql,jobid);
        }
    }
}
