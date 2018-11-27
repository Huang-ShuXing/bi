package com.maywide.bi.core.services;

import com.maywide.bi.util.DateUtils;
import com.maywide.bi.util.SqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TableUtil {

    private static final Logger log = LoggerFactory.getLogger(TableUtil.class);
    /***
     * 需要增加indexday 字段的表
     */
    private static List<String> NO_BACK_TABLE_LIST = null;
    static {
        TableUtil.NO_BACK_TABLE_LIST = new ArrayList<>();
        TableUtil.NO_BACK_TABLE_LIST.add("rpt_valid_sku_order");
        TableUtil.NO_BACK_TABLE_LIST.add("rpt_sku_order_index_change");
    }
    /***
     * 将表名增加日期 rpt_sku - > rpt_yymmdd
     * @param tableName
     * @param day
     * @return
     */
    public static String getDayTableName(String tableName,Date day) {
        return tableName + "_" + DateUtils.formatDate(day, DateUtils.FORMAT_YYYYMMDD);
    }

    /****
     * 将表名增加昨天的日期
     * 如 rpt_sku - > rpt_sku_20180804
     * @return
     */
    public static String getYesterdayTableName(String tableName) {
        return TableUtil.getDayTableName(tableName,DateUtils.getYesterday());
    }


    /***
     * 是否需要备份表
     * @param targetTable
     * @return
     */
    public static boolean needBack(String targetTable){
        return !TableUtil.NO_BACK_TABLE_LIST.contains(targetTable);
    }
}
