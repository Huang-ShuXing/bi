package com.maywide.bi.core.schedule;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

public class ScheduleJob {
    private long id ;
    private long scheduleid;
    private long jobid;
    private long sort ;
    private int status ;//1正常，0.停用
    private Date optime;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getScheduleid() {
        return scheduleid;
    }

    public void setScheduleid(long scheduleid) {
        this.scheduleid = scheduleid;
    }

    public long getJobid() {
        return jobid;
    }

    public void setJobid(long jobid) {
        this.jobid = jobid;
    }

    public long getSort() {
        return sort;
    }

    public void setSort(long sort) {
        this.sort = sort;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public Date getOptime() {
        return optime;
    }

    public void setOptime(Date optime) {
        this.optime = optime;
    }

    public static RowMapper<ScheduleJob> rowMapper(){
        return new RowMapper<ScheduleJob>() {
            @Override
            public ScheduleJob mapRow(ResultSet rs, int rowNum) throws SQLException {
                ScheduleJob sc = new ScheduleJob();
                sc.setId(rs.getInt("id"));
                sc.setScheduleid(rs.getInt("scheduleid"));
                sc.setJobid(rs.getInt("jobid"));
                sc.setSort(rs.getInt("sort"));
                sc.setStatus(rs.getInt("status"));
                sc.setOptime(rs.getTimestamp("optime"));
                return sc;
            }
        };
    }
}
