package com.maywide.bi.util;

import ch.qos.logback.core.util.TimeUtil;
import com.maywide.bi.core.execute.GenerateIndexOperation;
import com.maywide.bi.core.services.JdbcUtilServices;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;

public class SqlUtil {


    private static  Logger log = LoggerFactory.getLogger(SqlUtil.class);

    public  static final String TRACE="trace";
    private static final String DYNAMIC_DAY = "[dynamic_day]";
    private static final String SEARCH_DAY=":search_day";
    /***
     * 需要增加indexday 字段的表
     */
    /*public static List<String> INDEX_DAY_TABLES = null;*/
    public static String FIELD_INDEXDAY = "indexday";
   /* static {
        SqlUtil.INDEX_DAY_TABLES = new ArrayList<>();
        SqlUtil.INDEX_DAY_TABLES.add("rpt_sku_order_index");
        SqlUtil.INDEX_DAY_TABLES.add("rpt_sales_order_index");
        SqlUtil.INDEX_DAY_TABLES.add("rpt_valid_sku_order");
        SqlUtil.INDEX_DAY_TABLES.add("rpt_valid_sku_index");
        SqlUtil.INDEX_DAY_TABLES.add("rpt_ppv_order_index");
        SqlUtil.INDEX_DAY_TABLES.add("rpt_sku_order_index_change");
    }*/

    /**需要增加来源库字段的表*/
    /*private static List<String> SOURCE_DB_TABLES = null;*/
    public static String FIELD_SOURCE_DB= "sourcedb";
    /*static {
        SqlUtil.SOURCE_DB_TABLES = new ArrayList<>();
        SqlUtil.SOURCE_DB_TABLES.add("rpt_valid_sku_order");
    }*/
    /***
     * 统计SQL生成
     * @param sql
     * @return
     */
    public static String countSql(String sql)  {
        String countSql = "";
        if (sql.toUpperCase().indexOf("UNION") > 0
                || sql.toUpperCase().indexOf("INTERSECT") > 0) {
            countSql = "SELECT COUNT(*) FROM (" + sql + ")";
        } else {
            countSql = "SELECT COUNT(*) FROM "
                    + sql.substring(sql.toUpperCase().indexOf("FROM") + 4);
        }
        return countSql;
    }



    /***
     * 增加属性到列表
     * @param valueList
     * @param inparam
     * @return
     */
    public static List<Map<String,Object>> addOtherFiled(List<Map<String,Object>> valueList, HashMap<String,Object> inparam){
        if(null != valueList && !valueList.isEmpty()  && null != inparam && !inparam.isEmpty()){
            Map<String,Object> oneData = MapUtil.transferToLowerCase(valueList.get(0));
            Map<String,Object> realAddParam = new HashMap<>();
            for (Map.Entry<String,Object> entry : inparam.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if(oneData.containsKey(key.toLowerCase())){
                    //如果有这个字段，默认不添加
                }else {
                    realAddParam.put(key,value);
                }
            }
            if(!realAddParam.isEmpty()){
                for (Map<String, Object> one : valueList) {
                    for (Map.Entry<String,Object> entry : realAddParam.entrySet()) {
                        String key = entry.getKey();
                        Object value = entry.getValue();
                        one.put(key,value);
                    }
                }
            }
        }
        return valueList;
    }
    /***
     * 给SQL 增加偶数个时间条件，第一个条件是传入时间的0点，第二个条件是传入时间的23:59:59
     * 奇数个条件则原SQL 直接返回
     * @param sourceSql
     * @param targetDate
     * @return
     */
    public static String sqlAddTwoDate(String sourceSql,Date targetDate){
        int apperCharCount = StringUtils.countMatches(sourceSql,"?");
        if(apperCharCount > 0  ){
            if(apperCharCount % 2 != 0 ){
                log.error("SQL 配置 有误 ,SQL = ["+sourceSql+"]"  );
                return sourceSql;
            }
            int x = apperCharCount / 2;
            for (int i = 0; i < x; i++) {
                sourceSql = sourceSql.replaceFirst("\\?", "'"+DateUtils.formatDate(targetDate)+" 00:00:00'")
                        .replaceFirst("\\?","'"+DateUtils.formatDate(targetDate)+" 23:59:59'");
            }
        }
        return sourceSql;
    }

    /***
     * 给SQL 增加偶数个时间条件
     * @param sourceSql
     * @param startTime
     * @param endTime
     * @return
     */
    public static String sqlAddSETime(String sourceSql,Date startTime,Date endTime){
        int apperCharCount = StringUtils.countMatches(sourceSql,"?");
        if(apperCharCount > 0  ){
            if(apperCharCount % 2 != 0 ){
                log.error("SQL 配置 有误 ,SQL = ["+sourceSql+"]"  );
                return sourceSql;
            }
            int x = apperCharCount / 2;
            for (int i = 0; i < x; i++) {
                sourceSql = sourceSql.replaceFirst("\\?", "'"+DateUtils.formatTime(startTime)+"'")
                        .replaceFirst("\\?","'"+DateUtils.formatTime(endTime)+"'");
            }
        }
        return sourceSql;
    }

    /***
     * sql 中的时间字段替换
     * @param sourceSql
     * @param day
     * @return
     */
    public static String sqlDayTransform(String sourceSql,Date day){
        if(sourceSql.contains(SqlUtil.DYNAMIC_DAY)){
            sourceSql = sourceSql.replace(SqlUtil.DYNAMIC_DAY,"_"+ DateUtils.formatDate(day,DateUtils.FORMAT_YYYYMMDD));
        }

        if(sourceSql.contains(SqlUtil.SEARCH_DAY)){
            sourceSql = sourceSql.replace(SqlUtil.SEARCH_DAY,"'"+DateUtils.formatDate(day)+"'");
        }
        return sourceSql;
    }


    /**
     * 判断表是否含有indexDay 字段
     * @param targetTable
     * @return
     */
    /*public static boolean isHasIndexDay(String targetTable){
        if( SqlUtil.INDEX_DAY_TABLES.contains(targetTable)){
            return true;
        }else {
            return false;
        }
    }*/
}
