package com.maywide.bi.core.schedule;

import com.alibaba.fastjson.JSON;
import com.maywide.bi.util.DateUtils;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Objects;

public class Schedule {
    private long id ;
    //private long jobid ;
    private String name ;
    private Date startTime;
    private Date nextTime;
    private Date lastTime;
    private long spaceTime;
    private String status;
    private Date createTime;
    private Date optime;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
/*public long getJobid() {
        return jobid;
    }

    public void setJobid(long jobid) {
        this.jobid = jobid;
    }*/

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getNextTime() {
        return nextTime;
    }

    public void setNextTime(Date nextTime) {
        this.nextTime = nextTime;
    }

    public Date getLastTime() {
        if(null == lastTime){
            System.out.println("开始时间"+JSON.toJSON(getNextTime()));
            lastTime = new Date(getNextTime().getTime() - getSpaceTime() * 1000);
            System.out.println("上次时间"+JSON.toJSON(lastTime));
        }
        return lastTime;
    }

    public void setLastTime(Date lastTime) {
        this.lastTime = lastTime;
    }

    public long getSpaceTime() {
        return spaceTime;
    }

    public void setSpaceTime(long spaceTime) {
        this.spaceTime = spaceTime;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getOptime() {
        return optime;
    }

    public void setOptime(Date optime) {
        this.optime = optime;
    }


    public static RowMapper<Schedule> rowMapper(){
        return new RowMapper<Schedule>() {
            @Override
            public Schedule mapRow(ResultSet rs, int rowNum) throws SQLException {
                Schedule sc = new Schedule();
                sc.setId(rs.getInt("id"));
                //sc.setJobid(rs.getInt("job_id"));
                sc.setName(rs.getString("name"));
                sc.setStartTime(rs.getTimestamp("start_time"));
                sc.setNextTime(rs.getTimestamp("next_time"));
                sc.setLastTime(rs.getTimestamp("last_time"));
                sc.setSpaceTime(rs.getInt("space_time"));
                sc.setStatus(rs.getString("status"));
                sc.setCreateTime(rs.getTimestamp("create_time"));
                sc.setOptime(rs.getTimestamp("optime"));
                return sc;
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Schedule schedule = (Schedule) o;
        return id == schedule.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
