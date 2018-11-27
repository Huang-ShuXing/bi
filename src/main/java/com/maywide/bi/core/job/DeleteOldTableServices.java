package com.maywide.bi.core.job;

import com.maywide.bi.config.datasource.dynamic.Constants;
import com.maywide.bi.core.services.JdbcUtilServices;
import com.maywide.bi.core.services.TableUtil;
import com.maywide.bi.util.DateUtils;
import com.maywide.bi.util.SpringJdbcTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class DeleteOldTableServices implements  Runnable{

    @Autowired
    private JdbcUtilServices jdbcUtilServices;

    private static final Set<DeleteTableConfig> DELETE_TABLE_LIST = new HashSet<>();
    private static final Integer PRE_DAYS = 12;
    private static final Integer PRE_DEFAULT_DAYS = 5;
    static {
        DeleteTableConfig dtc = new DeleteTableConfig();
        dtc.setTalbeName("rpt_sku");
        dtc.setPreDays(5);
        DELETE_TABLE_LIST.add(dtc);

        dtc = new DeleteTableConfig();
        dtc.setTalbeName("rpt_user_order");
        dtc.setPreDays(5);
        DELETE_TABLE_LIST.add(dtc);

        dtc = new DeleteTableConfig();
        dtc.setTalbeName("rpt_asset");
        dtc.setPreDays(3);
        DELETE_TABLE_LIST.add(dtc);

        dtc = new DeleteTableConfig();
        dtc.setTalbeName("rpt_ppv_order");
        dtc.setPreDays(5);
        DELETE_TABLE_LIST.add(dtc);

        dtc = new DeleteTableConfig();
        dtc.setTalbeName("rpt_ppv_order_index_change");
        dtc.setPreDays(5);
        DELETE_TABLE_LIST.add(dtc);

        dtc = new DeleteTableConfig();
        dtc.setTalbeName("rpt_valid_day_sku_order");
        dtc.setPreDays(5);
        DELETE_TABLE_LIST.add(dtc);

        dtc = new DeleteTableConfig();
        dtc.setTalbeName("rpt_valid_ppv_order");
        dtc.setPreDays(5);
        DELETE_TABLE_LIST.add(dtc);

    }

    public static void addDelTableSet(String tablename,int preDays){
        if(!DELETE_TABLE_LIST.contains(tablename)){
            DeleteTableConfig dtc = new DeleteTableConfig();
            dtc.setTalbeName(tablename);
            dtc.setPreDays(preDays);
            DELETE_TABLE_LIST.add(dtc);
        }
    }

    public static void addDelTableSet(String tablename){
        addDelTableSet(tablename,PRE_DEFAULT_DAYS);
    }


    @Autowired
    private SpringJdbcTemplate springJdbcTemplate;

    public void deleteTable(){
        Set<DeleteTableConfig> set =  DELETE_TABLE_LIST;
        if(!set.isEmpty()){
            for (DeleteTableConfig deleteTableConfig : set) {
                this.deleteTable(deleteTableConfig);
            }
        }
    }

    public void deleteTable(DeleteTableConfig dtc){
        String tableName = dtc.getTalbeName();
        int preDays = dtc.getPreDays();
        Date today = new Date();
        for (Integer i = 0; i < DeleteOldTableServices.PRE_DAYS; i++) {
            Date targetDate = DateUtils.addNday(today,0-preDays-i);
            String dayTableName = TableUtil.getDayTableName(tableName,targetDate);
            if(jdbcUtilServices.existTable(Constants.DEFAULT_DATA_SOURCE_NAME,dayTableName)){
                jdbcUtilServices.dropTable(Constants.DEFAULT_DATA_SOURCE_NAME,dayTableName);
            }
        }
    }

    @Override
    public void run() {
        this.deleteTable();
    }
}

class DeleteTableConfig{
    /**表名*/
    private String talbeName ;
    /**保留前几天*/
    private int preDays;

    public String getTalbeName() {
        return talbeName;
    }

    public void setTalbeName(String talbeName) {
        this.talbeName = talbeName;
    }

    public int getPreDays() {
        return preDays;
    }

    public void setPreDays(int preDays) {
        this.preDays = preDays;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeleteTableConfig that = (DeleteTableConfig) o;
        return Objects.equals(talbeName, that.talbeName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(talbeName);
    }
}

