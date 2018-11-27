package com.maywide.bi.core.job;

import com.maywide.bi.core.services.JdbcUtilServices;
import com.maywide.bi.util.SpringJdbcTemplate;
import org.springframework.beans.factory.annotation.Autowired;

import javax.print.attribute.standard.RequestingUserName;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Job {
    @Autowired
    protected JdbcUtilServices jdbcUtilServices;
    @Autowired
    protected SpringJdbcTemplate springJdbcTemplate;
    public abstract Object excuteJob(int jobid);

    //TODO 获取任务的详细信息
    //TODO 组装任务的详细信息
    public static ConcurrentHashMap<Long,AtomicInteger> RUNNING_JOB_COUNT_HASH = new ConcurrentHashMap<>();

}
