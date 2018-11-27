package com.maywide.bi.core.services;

import com.maywide.bi.config.datasource.dynamic.DbContextHolder;
import com.maywide.bi.util.DateUtils;
import com.maywide.bi.util.MapUtil;
import com.maywide.bi.util.SpringJdbcTemplate;
import com.maywide.bi.util.SqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.*;
import java.util.*;
import java.util.Date;

@Service
public class JdbcUtilServices {

    private static Logger log = LoggerFactory.getLogger(JdbcUtilServices.class);
    @Autowired
    private SpringJdbcTemplate springJdbcTemplate;

    //锁对象
    private static Object lock = new Object();

    /****
     * 将表名增加今日的日期
     * 如 rpt_sku - > rpt_sku_20180804
     * @return
     */
    /*public static String getTodayTableName(String tableName){
        return  tableName + "_" +DateUtils.formatDate(new Date(),DateUtils.FORMAT_YYYYMMDD);
    }*/




    /***
     * 批量插入数据库，同时插入一直备份表(备份表名= 表+昨天的日期(table_yymmdd))
     * @param inDataSource
     * @param targetTable
     * @param valueList
     * @return
     */
    /*@Transactional
    public boolean batchInsertAndBackYesTable(String inDataSource,String targetTable , List<Map<String,Object>> valueList){
        Date yesterday = org.apache.commons.lang.time.DateUtils.addDays(new Date(),-1);
        return this.batchInsertAndBack(inDataSource,targetTable,valueList,yesterday);
    }*/

    /***
     * 批量插入数据库，同时插入一直备份表(备份表名= 表+指定日期(table_yymmdd))
     * @param inDataSource
     * @param targetTable
     * @param valueList
     * @param day
     * @return
     */
    @Transactional
    public boolean batchInsertAndBack(String inDataSource,String targetTable , List<Map<String,Object>> valueList,Date day){
        String todayStr = DateUtils.formatDate(day,DateUtils.FORMAT_YYYYMMDD);
        String backTable = targetTable + "_" +todayStr;
        this.batchInsert(inDataSource,backTable,valueList);
        this.batchInsert(inDataSource,targetTable,valueList);
        return true;
    }


    @Transactional
    public int[] batchInsert(String inDataSource,String targetTable , List<Map<String,Object>> valueList){
        if(null == valueList || valueList.isEmpty()){
            log.info("准备插入数到+"+inDataSource+",table ="+targetTable+",数量为空,不执行插入");
            return null;
        }
        log.info("准备插入数到 "+inDataSource+",table ="+targetTable+",数量="+valueList.size());
        StringBuffer insertSql =new StringBuffer( "  insert into "+targetTable+" ( ");

        StringBuffer paramSb= new StringBuffer();
        Map<String,Object> oneMap = valueList.get(0);
        Object[] keys = oneMap.keySet().toArray();
        for (int i = 0; i < keys.length; i++) {
            if(i == 0 ){
                insertSql.append(keys[i]);
                paramSb.append("?");
            }else {
                insertSql.append(","+keys[i]);
                paramSb.append(",?");
            }
        }
        insertSql.append(") ").append(" values ( ").append(paramSb.toString()).append(" ) ");
        log.info("即将执行的SQL =" +insertSql);
        long t1 = System.currentTimeMillis();
        int[] result = null;
        try{
            DbContextHolder.setDBType(inDataSource);
            result =  springJdbcTemplate.batchUpdate(insertSql.toString(), new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                    Map<String,Object> oneMap = valueList.get(i);
                    for (int j = 0; j < keys.length; j++) {
                        preparedStatement.setObject(j+1,oneMap.get(keys[j]));
                    }
                }
                @Override
                public int getBatchSize() {
                    return valueList.size();
                }
            });
            long t2 = System.currentTimeMillis();
            log.info("批量插入- [成功] - "+inDataSource+",table ="+targetTable+",数量="+valueList.size()+"耗时:["+(t2-t1)+"ms]");
        }catch (Exception e){
            log.error(e.getMessage(),e);
        }

        return result;
    }

    /***
     * 备份表，将指定的表全表备份，新的表名为 table_name_yyyymmdd
     * @param inDataSource
     * @param targetTable
     */
    public void backUpTable(String inDataSource, String targetTable,Date targetDate) {
        String newTable = TableUtil.getDayTableName(targetTable,targetDate);
        if(this.validateTableNameExistByCon(inDataSource,newTable)){
            //删除表
            this.dropTable(inDataSource,newTable);
        }
        log.info("备份表 - 库["+inDataSource+"]的表:["+targetTable+"]到["+newTable+"]......开始");
        //备份数据
        String backUpSql = " create table "+ newTable +" as SELECT * FROM " + targetTable;
        DbContextHolder.setDBType(inDataSource);
        int x = springJdbcTemplate.update(backUpSql);
        log.info("备份表 - 库["+inDataSource+"]的表:["+targetTable+"]到["+newTable+"].......成功");
    }

    /***
     * 验证表存在否
     * @param datasource
     * @param table
     * @return
     */
    public boolean existTable(String datasource,String table){
        return this.validateTableNameExistByCon(datasource,table);
    }
    /***
     * 复制表结构
     * @param datasource
     * @param newTable
     * @param oldTable
     */
    public void copyTableNoData(String datasource,String oldTable,String newTable){
        synchronized (JdbcUtilServices.lock){
            if (!this.existTable(datasource,newTable)){
                DbContextHolder.setDBType(datasource);
                String copySql =" create table "+ newTable +" as SELECT * FROM " + oldTable +" where 1 = 2";
                int x = springJdbcTemplate.update(copySql);
                log.info(" 数据库["+datasource+"]的 【"+newTable+"】创建成功");

                Connection con = null;
                ResultSet rs =null;
                try {
                    //如果有id 设置为主键，自增
                    con =  springJdbcTemplate.getDataSource().getConnection();
                    DatabaseMetaData dmd =con.getMetaData();
                    rs =dmd.getColumns(null, null, newTable, null);
                    while(rs.next()){
                        String columnName = rs.getString("COLUMN_NAME");  //列名
                        if("id".equalsIgnoreCase(columnName)){
                            String addIncrSql = " ALTER TABLE " + newTable +  " MODIFY COLUMN `id`  int(11) NOT NULL AUTO_INCREMENT FIRST , ADD PRIMARY KEY (`id`) ";
                            springJdbcTemplate.execute(addIncrSql);
                        }
                    }
                } catch (SQLException e) {
                    log.warn("设置表自增的时候出错了，但是不要紧。"+e);
                }finally {
                    try {
                        if(null != rs && !rs.isClosed()){
                            rs.close();
                        }
                        if(null != con && !con.isClosed()){
                            con.close();
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    /***
     * 按日期复制表结构
     * @param datasource
     * @param oldTable
     */
    public void copyTableNoDataByYesterday(String datasource,String oldTable){
        this.copyTableNoData(datasource,oldTable,TableUtil.getYesterdayTableName(oldTable));
    }





    /***
     * 判断数据库连接的表是否存在
     * @param jt
     * @param tableName
     * @return
     */
    /*public static boolean validateTableNameExist(JdbcTemplate jt,String tableName) {
        int tableNum = jt.queryForObject("SELECT COUNT(*) FROM ALL_TABLES WHERE TABLE_NAME=" + tableName,Integer.class);
        if (tableNum > 0) {
            return true;
        }else {
            return false;
        }
    }*/

    /***
     * 通过Connecion获取表是否存在
     * @param inDataSource 数据库名
     * @param tableName    表明
     * @return
     */
    public  boolean  validateTableNameExistByCon(String inDataSource,String tableName) {
        DbContextHolder.setDBType(inDataSource);
        boolean isExist = false;
        Connection con = null;
        ResultSet rs =null;
        try {
            con = this.springJdbcTemplate.getDataSource().getConnection();
            rs = con.getMetaData().getTables(null, null, tableName, null);
            if (rs.next()) {
                isExist =  true;
            } else {
                isExist =  false;
            }
        } catch (SQLException e) {
            log.info(e.getMessage(),e);
            e.printStackTrace();
        }finally {
            try {
                if(null != rs && !rs.isClosed()){
                    rs.close();
                }
                if(null != con && !con.isClosed()){
                    con.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
        log.info("判断表存在 - 表 "+tableName +"在不在 数据库["+inDataSource+"]中 = 【"+isExist+"】");
        return isExist;
    }


    /***
     * 清除数据库的表数据
     * @param inDataSource
     * @param targetTable
     */
    public void clearTable(String inDataSource, String targetTable) {
        String delAllData = "delete from "+targetTable ;
        DbContextHolder.setDBType(inDataSource);
        int r = springJdbcTemplate.update(delAllData);
        log.info("清空数据 - 库["+inDataSource+"]中的表:[ " + targetTable+"]") ;
    }

    /***
     * 删除指定数据库的表
     * @param inDataSource
     * @param targetTable
     */
    public void dropTable(String inDataSource, String targetTable) {
        String dropTable = "drop table "+targetTable ;
        DbContextHolder.setDBType(inDataSource);
        springJdbcTemplate.execute(dropTable);
        log.info("删除表 - 库["+inDataSource+"]中的表:[ " + dropTable+"]") ;
    }

    /***
     * 解析SQL 获取数据
     * @param sourceDataSource
     * @param sourceSql
     * @return
     */
    public List<Map<String,Object>> getListBySql(String sourceDataSource,String sourceSql){
        long t1 = System.currentTimeMillis();
        DbContextHolder.setDBType(sourceDataSource);
        List<Map<String,Object>> list = springJdbcTemplate.queryForList(sourceSql);
        long t2 = System.currentTimeMillis();
        log.info("[查询SQL] - 耗时  ["+ (t2 - t1)+"]");
        return list;
    }

    /***
     *
     * @param sourceDataSource
     * @param sql
     * @return
     */
    public int count(String sourceDataSource,String sql){
        DbContextHolder.setDBType(sourceDataSource);
        return springJdbcTemplate.queryForObject(SqlUtil.countSql(sql), Integer.class);
    }


    /***
     * 执行数据库的SQL
     * @param dataSource
     * @param sql
     */
    public void execute(String dataSource,String sql ){
        DbContextHolder.setDBType(dataSource);
        springJdbcTemplate.execute(sql);
    }


    /***
     * 生成变动轨迹表
     * @param datasource
     * @param targetTable
     * @return
     */
    public String createSaveTraceLogSql(String datasource, String targetTable, HashMap<String,String> inParam,HashMap<String,String> whereParam){
        //1.明细表都是要按要求 targetTable+"_detail"的形式命名，否则直接返回
        //2.查询表有什么字段，全部备份，除了optime,源表id 字段改为sid,detail表字段一定要有一个最好设置一个自增长的id
        //3.生成sql
        List<String> columns = this.getColumnsInfo(datasource,targetTable);
        if(columns != null && !columns.isEmpty()){
            String traceTable = targetTable +"_" + SqlUtil.TRACE;
            StringBuffer sbSql = new StringBuffer(" insert into " + traceTable +"( ");
            StringBuffer selectSql = new StringBuffer(" select  ");
            for (int i = 0; i < columns.size(); i++) {
                if(inParam.containsKey(columns.get(i).toLowerCase())){
                    sbSql.append(" " + columns.get(i)+" ");
                    selectSql.append(" "+inParam.get(columns.get(i).toLowerCase())+ " as "+columns.get(i)+" ");
                }else {
                    if("id".equals(columns.get(i))){
                        sbSql.append(" sid");
                        selectSql.append(" "+columns.get(i)+"");
                    }else{
                        sbSql.append(" "+columns.get(i)+"");
                        selectSql.append(" "+columns.get(i)+"");
                    }
                }
                if( i < columns.size() - 1){
                    sbSql.append(" , ");
                    selectSql.append(" , ");
                }
            }
            selectSql.append(" from "+ targetTable);
            if(null != whereParam && !whereParam.isEmpty()){
                selectSql.append(" where 1 = 1");
                for (Map.Entry<String,String> entry : whereParam.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    selectSql.append(" and "+key + "="+value);
                }
            }
            sbSql.append(" )   ( " ).append(selectSql).append(" )");
            return sbSql.toString();
        }else {
            return null;
        }


    }


    /**
     * 获取表的列信息
     * @param datasource
     * @param tablename
     * @return
     * @throws SQLException
     */
    public  List<String> getColumnsInfo(String datasource, String tablename)  {
        List<String> list = new ArrayList<>();
        DbContextHolder.setDBType(datasource);
        Connection conn =  null;
        ResultSet rs = null;
        try{
            /**
             * 设置连接属性,使得可获取到列的REMARK(备注)
             */
            conn = this.springJdbcTemplate.getDataSource().getConnection();
            DatabaseMetaData dbmd = conn.getMetaData();
            /**
             * 获取可在指定类别中使用的表列的描述。
             * 方法原型:ResultSet getColumns(String catalog,String schemaPattern,String tableNamePattern,String columnNamePattern)
             * catalog - 表所在的类别名称;""表示获取没有类别的列,null表示获取所有类别的列。
             * schema - 表所在的模式名称(oracle中对应于Tablespace);""表示获取没有模式的列,null标识获取所有模式的列; 可包含单字符通配符("_"),或多字符通配符("%");
             * tableNamePattern - 表名称;可包含单字符通配符("_"),或多字符通配符("%");
             * columnNamePattern - 列名称; ""表示获取列名为""的列(当然获取不到);null表示获取所有的列;可包含单字符通配符("_"),或多字符通配符("%");
             */
            rs =dbmd.getColumns(null, null, tablename, null);
            while(rs.next()){
                String columnName = rs.getString("COLUMN_NAME");  //列名
                list.add(columnName);
            }
        }catch(SQLException ex){
            ex.printStackTrace();
        }finally{
            try {
                if(null != rs && !rs.isClosed()){
                    rs.close();
                }
                if(null != conn && !conn.isClosed()){
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    private static Date columnMapDate = new Date();
    private static final Map<String, Boolean> columnMap = Collections.synchronizedMap(new HashMap<>());

    /***
     * 判断数据库中的表有没有某字段
     * @param db
     * @param table
     * @param column
     * @return
     */
    public boolean tableHasColumn(String db,String table,String column){
        Date nowDate = new Date();
        if(nowDate.after(JdbcUtilServices.columnMapDate)){
            columnMap.clear();
            JdbcUtilServices.columnMapDate = org.apache.commons.lang.time.DateUtils.addMinutes(nowDate,30);
        }
        String key = db + "-"+table+"-"+column;
        if(columnMap.containsKey(key)){
            return columnMap.get(key);
        }else {
            List<String> columnList = this.getColumnsInfo(db,table);
            boolean f = false;
            if(null != columnList && !columnList.isEmpty() ){
                for (String s : columnList) {
                    if(column.equalsIgnoreCase(s)){
                        f = true;
                    }
                }
            }
            columnMap.put(key,f);
            return f;
        }
    }

    /**
     * 增加额外字段的插入
     * @param targetDb
     * @param targetTable
     * @param sourceBb
     * @param targetDate
     * @param valueList
     * @param inparam
     * @return
     */
    public List<Map<String,Object>> addOtherFiled(String targetDb ,String targetTable,String sourceBb, Date targetDate, List<Map<String,Object>> valueList,HashMap<String,Object> inparam){
        if(null != valueList && !valueList.isEmpty() ){
            boolean inIndexDay = false;
            boolean inSourceBb = false;
            if( this.tableHasColumn(targetDb,targetTable,SqlUtil.FIELD_INDEXDAY)){
                log.debug("表{}增加{}字段",targetTable,SqlUtil.FIELD_INDEXDAY);
                inIndexDay = true;
            }
            if( this.tableHasColumn(targetDb,targetTable,SqlUtil.FIELD_SOURCE_DB)){
                log.debug("表{}增加{}字段",targetTable,SqlUtil.FIELD_SOURCE_DB);
                inSourceBb = true;
            }
            //额外属性
            Map<String,Object> realAddParam = new HashMap<>();
            if(null != inparam && !inparam.isEmpty()){
                Map<String,Object> oneData = MapUtil.transferToLowerCase(valueList.get(0));
                for (Map.Entry<String,Object> entry : inparam.entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    if(oneData.containsKey(key.toLowerCase())){
                        //如果有这个字段，默认不添加
                    }else {
                        realAddParam.put(key,value);
                    }
                }
            }
            for (Map<String, Object> one : valueList) {
                if(inIndexDay) {
                    one.put(SqlUtil.FIELD_INDEXDAY, DateUtils.formatDate(targetDate));
                }
                if(inSourceBb){
                    one.put(SqlUtil.FIELD_SOURCE_DB,sourceBb);
                }
                for (Map.Entry<String,Object> entry : realAddParam.entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    one.put(key,value);
                }
            }
        }
        return valueList;
    }

}
