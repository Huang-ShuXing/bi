package com.maywide.bi.core.services;

import com.alibaba.druid.support.json.JSONUtils;
import com.maywide.bi.config.datasource.dynamic.Constants;
import com.maywide.bi.config.datasource.dynamic.DbContextHolder;
import com.maywide.bi.core.execute.OperationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class JobServices {

    private Logger log = LoggerFactory.getLogger(JobServices.class);
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private JdbcUtilServices jdbcUtilServices;
   @Autowired
   private OperationFactory operationFactory;


    /*public  void excute(){
        AtomicInteger ai1 = new AtomicInteger(1);
        AtomicInteger ai2 = new AtomicInteger(1);
        AtomicInteger ai3 = new AtomicInteger(1);
        for (int i = 0; i < 1000; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    String[] sqls =new String[1000];
                    long t1 = System.currentTimeMillis();
                    for (int i1 = 0; i1 < 500; i1++) {
                        String serialno = String.valueOf(ai1.incrementAndGet());
                        String logid = String.valueOf(ai2.incrementAndGet());
                        String ibizlog = " INSERT INTO `biz_log` (`logid`, `serialno`, `orderid`, `custid`, `tsp`, `servid`, `opcode`, `optime`,"+
                                " `systemid`, `operator`, `deptid`, `spay`, `isback`, `rbserialno`, `ispay`, `paytime`, `payway`, "+
                                " `rpay`, `assist`, `expiretime`, `expirestatus`, `memo`) VALUES "+
                                " ('"+logid+"', '"+serialno+"', '2', '5', 'telecom', '5', 'BIZ_ISPOSTPONE', '2017-09-04 09:59:16', 'AAA', NULL, NULL, '10.00', 'N', NULL, 'Y', '2017-09-04 10:06:31', 'FFFFFFFFFFFF', '10.00', NULL, '2017-09-05 09:59:16', '0', NULL)";
                        String agreeid = String.valueOf(ai3.incrementAndGet());
                        String ibs = " INSERT INTO `biz_sku_trace` (`agreeid`, `custid`, `servid`, `salesid`, `skuid`, `serialno`, `stime`,"+
                                " `etime`, `stdfee`, `rstdfee`, `fees`, `cycle`, `count`, `unit`, `ostatus`, `ispostpone`, "+
                                " `feecode`, `ifeecode`, `rfeecode`, `salespkgid`, `salespkginsid`, `chagemode`,"+
                                " `isroll`, `ispay`, `paytime`, `optime`, `expirestatus`) VALUES " +
                                "      ('"+agreeid+"', '2', '2', '65703', '65701', '138725', '2018-01-25 09:28:20', '2018-02-24 09:28:20', '0.0100', '0.0100', '0.01', '1', '4', '1', '0', 'Y', '35575', 'A001', 'PP01', NULL, NULL, NULL, 'N', 'Y', '2018-01-25 10:38:58', '2018-01-25 09:28:20', '1')";
                        sqls[i1] =ibizlog;
                        sqls[500+i1] = ibs;
                    }
                    DbContextHolder.setDBType("60node1");
                    jdbcTemplate.batchUpdate(sqls);
                    long t2 = System.currentTimeMillis();
                    System.out.println("耗时：【"+(t2-t1)+"】");
                }
            }).start();


        }
        //1.查询任务信息
        //2.查询表还有SQL
        //3.查询源库还有目标库
        //4.SQL 条件赋值
        //5.执行逻辑
        String jobSql = "select * from bi_job_info where status =1";
        this.runJob(jobSql);
    }

    private void runJob(String jobSql){
        runJob(jobSql,null);
    }*/

    private void runJob(int jobid,Date startTime,Date endTime){
        DbContextHolder.setDBType(Constants.DEFAULT_DATA_SOURCE_NAME);
        String jobSql = "select * from bi_job_info where id = "+jobid;
        List<Map<String,Object>> list = jdbcTemplate.queryForList(jobSql);
        if(null != list && !list.isEmpty()){
            //TODO 循环
            for (int i = 0; i < list.size(); i++) {
                Integer id = (Integer) list.get(i).get("id");
                String target_table = (String) list.get(i).get("target_table");
                String source_sql = (String) list.get(i).get("source_sql");
                Long way = (Long) list.get(i).get("way");
                String jobDbSql = "select * from bi_job_datasource where job_id = ? ";
                List<Map<String,Object>> job_db_list =  jdbcTemplate.queryForList(jobDbSql,id);
                if(null == job_db_list ||job_db_list.isEmpty()){
                    log.error("job.id = "+id+",job.job_code = " + (String) list.get(i).get("job_code")+",没有配置数据库" );
                    continue;
                }

                log.info("job 关联的所有库信息"+JSONUtils.toJSONString(job_db_list));

                //区别配置的目标库 与 源库
                List<Integer> targetDbList = new ArrayList<>();
                List<Integer> sourceDbList = new ArrayList<>();
                for (int x = 0; x < job_db_list.size(); x++) {
                    if(job_db_list.get(x).get("type").toString().equals("0")){
                        sourceDbList.add((Integer) job_db_list.get(x).get("db_id"));
                    }else {
                        targetDbList.add((Integer) job_db_list.get(x).get("db_id"));
                    }
                }

                if(targetDbList.isEmpty()){
                    log.error("job.id = "+id+",job.job_code = " + (String) list.get(i).get("job_code")+",没有配置目标库" );
                    continue;
                }else {
                    System.out.println("目标库："+JSONUtils.toJSONString(targetDbList));
                }

                if(sourceDbList.isEmpty()){
                    log.error("job.id = "+id+",job.job_code = " + (String) list.get(i).get("job_code")+",没有配置源库" );
                    continue;
                }else{
                    System.out.println("源库："+JSONUtils.toJSONString(sourceDbList));
                }


                String getDbSql = "select * from bi_data_source where id in (:datasourceId) ";
                NamedParameterJdbcTemplate namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
                Map<String, Object> params = new HashMap<String, Object>();
                //查询所有目标库
                params.put("datasourceId",targetDbList);
                List<Map<String, Object>> targetDb = namedParameterJdbcTemplate.queryForList(getDbSql,params);

                //查询所有源库
                params.put("datasourceId",sourceDbList);
                List<Map<String, Object>> sourceDb = namedParameterJdbcTemplate.queryForList(getDbSql,params);

                for (Map<String, Object> targetBdOne : targetDb) {
                    System.out.println("目标库:"+JSONUtils.toJSONString(targetBdOne));
                    //TODO 这里需要优化，如果多个目标库，就多查询了源库
                    for (Map<String, Object> sourceBdOne : sourceDb) {
                        if(sourceBdOne.containsKey("c_name")){
                            String sourceDatasource = (String) sourceBdOne.get("c_name");
                            String targetDatasource = (String) targetBdOne.get("c_name");
                            operationFactory.getOperation(way.intValue()).execute(sourceDatasource,source_sql,targetDatasource,target_table,startTime,endTime);//sourceDatasource,source_sql,
                        }
                    }
                }
            }
        }
    }

    /***
     * 执行指定的任务
     * @param jobid
     */
    /*public void runJobById(int jobid){
        String jobSql = "select * from bi_job_info where id = "+jobid;
        this.runJob(jobSql);
    }*/

    /***
     * 指定时间执行 任务
     * @param jobid
     * @param startTime
     * @param endTime
     */
    public void runJobByIdAndDate(int jobid,Date startTime,Date endTime){
        this.runJob(jobid,startTime,endTime);
    }
}
