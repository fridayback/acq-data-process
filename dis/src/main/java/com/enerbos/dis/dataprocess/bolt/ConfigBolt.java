/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * ConfigBolt.java
 * Created on  2018/1/16 15:59
 * 版本       修改时间          作者      修改内容
 *            2018/1/16      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.bolt;/*
 * ConfigBolt
 * Created on  202018/1/16 15:59  by liulin
 *  ${COPYRIGHT}
 */

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.enerbos.dis.dataprocess.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

import java.util.*;

public class ConfigBolt implements IRichBolt {
    public static final String CLEAN_RULE = CleanRule.class.getSimpleName();//"clean";
    public static final String CALCULATE_RULE = CalculateRule.class.getSimpleName();//"calculate";
    public static final String SAVE_RULE = SaveRule.class.getSimpleName();//"save";
    public static final String TIMEOUT_RULE = DataTimeOutRule.class.getSimpleName();//"timeout";
    public static final String ORG= "org";
    public static final String SITE = "site";


    private OutputCollector collector;
    private int taskId;
    private Map properties;
    private Logger log = LoggerFactory.getLogger(ConfigBolt.class);
    private Jedis jedis;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        taskId = topologyContext.getThisTaskId();
        properties =map;
        String redisHost = map.get("redis-host").toString();
        String redisHostArray[] = redisHost.split(":");
        jedis = new Jedis(redisHostArray[0],new Integer(redisHostArray[1]).intValue());
        //jedis.auth("");
        //jedis.connect();
    }

    @Override
    public void execute(Tuple tuple) {
        log.trace("-------- process config data:{}",tuple);
        List cfgs = (ArrayList<Map>)tuple.getValue(0);
        if (!updateAcqConfig(cfgs)) {
            collector.fail(tuple);
            return;
        }
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("calculate-config", new Fields("tagid","data","type"));
        outputFieldsDeclarer.declareStream("clean-config", new Fields("tagid","data","type"));
        outputFieldsDeclarer.declareStream("save-config", new Fields("tagid","data","type"));
        outputFieldsDeclarer.declareStream("timeout-config", new Fields("tagid","data","type"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private boolean updateAcqConfig(List cfgs){

        List<PointCfg> pointConfigList = cfgs;

        HashMap<Integer,ArrayList> calculateConfigMap = new HashMap<>();
        HashMap<Integer,ArrayList> cleanConfigMap = new HashMap<>();
        HashMap<Integer,ArrayList> timeoutConfigMap = new HashMap<>();
        HashMap<Integer,ArrayList> saveConfigMap = new HashMap<>();
        for (PointCfg p:pointConfigList){
            CleanRule cleanRule;
            CalculateRule calculateRule;
            SaveRule saveRule;
            DataTimeOutRule dataTimeOutRule;
            try {
                cleanRule = new CleanRule(p.getPtClean(),p.getOrg(),p.getSite(),p.getTagId());
            } catch (Exception e) {
                cleanRule = null;
                log.error("bad clean rule:",p.getPtClean());
            }
            try {
                calculateRule = new CalculateRule(p.getFormula(),p.getOrg(),p.getSite(),p.getTagId());
            } catch (Exception e) {
                calculateRule = null;
                log.error("bad calculate rule:",p.getFormula());
            }
            try {
                saveRule = new SaveRule(p.getPtSave(),p.getOrg(),p.getSite(),p.getTagId());
            } catch (Exception e) {
                saveRule = null;
                log.error("bad save rule:",p.getPtSave());
            }
            try {
                dataTimeOutRule = new DataTimeOutRule(p.getPtTimeout(),p.getOrg(),p.getSite(),p.getTagId());
            } catch (Exception e) {
                dataTimeOutRule = null;
                log.error("bad timeout rule:",p.getPtTimeout());
            }

            Map rules = new HashMap<String,String>();
            rules.put(ORG,p.getOrg());
            rules.put(SITE,p.getSite());
            rules.put(CLEAN_RULE,p.getPtClean());
            rules.put(CALCULATE_RULE,p.getFormula());
            rules.put(SAVE_RULE,p.getPtSave());
            rules.put(TIMEOUT_RULE,p.getPtTimeout());
            try{
                jedis.connect();
                jedis.hmset(p.getTagId(),rules);
            } catch (JedisException e){
                log.error("",rules,e);
                return false;
            }
            Integer calculateId = new Integer(GroupingMethod.getGroupIdByKey(p.getTagId(),GroupingMethod.dataCalculateBoltCount));
            ArrayList calculateConfigs = calculateConfigMap.get(calculateId);
            if(calculateConfigs == null){
                calculateConfigs = new ArrayList();
                calculateConfigMap.put(calculateId,calculateConfigs);
            }
            calculateConfigs.add(calculateRule);

            Integer cleanId = new Integer(GroupingMethod.getGroupIdByKey(p.getTagId(),GroupingMethod.dataCleanBoltCount));
            ArrayList cleanConfigs = cleanConfigMap.get(cleanId);
            if(calculateConfigs == null){
                cleanConfigs = new ArrayList();
                cleanConfigMap.put(cleanId,cleanConfigs);
            }
            cleanConfigs.add(cleanRule);

            Integer timeoutId = new Integer(GroupingMethod.getGroupIdByKey(p.getTagId(),GroupingMethod.rtdataSaveBoltCount));
            ArrayList timeoutConfigs = timeoutConfigMap.get(timeoutId);
            if(timeoutConfigs == null){
                timeoutConfigs = new ArrayList();
                timeoutConfigMap.put(timeoutId,timeoutConfigs);
            }
            timeoutConfigs.add(dataTimeOutRule);

            Integer saveId = new Integer(GroupingMethod.getGroupIdByKey(p.getTagId(),GroupingMethod.hisdataSaveBoltCount));
            ArrayList saveConfigs = saveConfigMap.get(saveId);
            if(calculateConfigs == null){
                saveConfigs = new ArrayList();
                saveConfigMap.put(saveId,saveConfigs);
            }
            saveConfigs.add(saveRule);

        }

        Iterator itorCalculate = calculateConfigMap.entrySet().iterator();
        while(itorCalculate.hasNext()){
            Map.Entry entry = (Map.Entry) itorCalculate.next();
            collector.emit("calculate-config",new Values(entry.getKey(),entry.getValue(),DisMessage.MS_ACQCFGCMD));
        }

        Iterator itorClean = cleanConfigMap.entrySet().iterator();
        while(itorClean.hasNext()){
            Map.Entry entry = (Map.Entry) itorClean.next();
            collector.emit("clean-config",new Values(entry.getKey(),entry.getValue(),DisMessage.MS_ACQCFGCMD));
        }

        Iterator itorTimeout = timeoutConfigMap.entrySet().iterator();
        while(itorTimeout.hasNext()){
            Map.Entry entry = (Map.Entry) itorTimeout.next();
            collector.emit("timeout-config",new Values(entry.getKey(),entry.getValue(),DisMessage.MS_ACQCFGCMD));
        }

        Iterator itorSave = saveConfigMap.entrySet().iterator();
        while(itorSave.hasNext()){
            Map.Entry entry = (Map.Entry) itorSave.next();
            collector.emit("save-config",new Values(entry.getKey(),entry.getValue(),DisMessage.MS_ACQCFGCMD));
        }
//        if(null != cleanRule) collector.emit("clean-config",new Values(p.getTagId(),cleanRule,DisMessage.MS_ACQCFGCMD));
//        if(null != calculateRule) collector.emit("calculate-config",new Values(p.getTagId(),calculateRule,DisMessage.MS_ACQCFGCMD));
//        if(null != dataTimeOutRule) collector.emit("timeout-config",new Values(p.getTagId(),dataTimeOutRule,DisMessage.MS_ACQCFGCMD));
//        if(null != saveRule) collector.emit("save-config",new Values(p.getTagId(),saveRule,DisMessage.MS_ACQCFGCMD));

        return true;
    }

//    private boolean saveConfig(AcqConfig cfg){
//
//        Connection conn =null;
//        Statement st=null;
//
//        ResultSet rs=null;
//
//        PreparedStatement ps=null;
//        String driver = "com.mysql.jdbc.Driver";
//
//        // URL指向要访问的数据库名scutcs
//        String dbHost = (String) properties.get("db-host");
//        String dbName = (String) properties.get("db-name");
//
//
//        // MySQL配置时的用户名
//        String dbUser = (String) properties.get("db-user");
//        // MySQL配置时的密码
//        String dbPassword = (String) properties.get("db-password");
//        String url = "jdbc:mysql://"+dbHost+"/"+dbName;
//
//        try {
//            Class.forName(driver);
//
//        conn = DriverManager.getConnection(url, dbUser, dbPassword);
//        conn.setAutoCommit(false);
//        if(conn.isClosed()){
//            log.error("saveConfige failed: connection to db:{} is closed",dbHost);
//            return false;
//        }
//        ArrayList<PointCfg> ptcfgs = cfg.getPoints();
//        String sqlTemplate = "insert into  point_effective(tagid,pt_clean,pt_save,pt_timeout,formula,org,site) values('##TAGID##','##PT_CLEAN##','##PTSAVE##','##PTTIMEOUT##','##FORMULA##','##ORG##','##SITE##') ON DUPLICATE KEY UPDATE pt_clean=VALUES(pt_clean),pt_save=VALUES(pt_save),pt_timeout=VALUES(pt_timeout),formula=VALUES(formula);";
//        for (PointCfg ptcfg:ptcfgs){
//            String sql = sqlTemplate.replace("##TAGID##",ptcfg.getTagId());
//            sql = sqlTemplate.replace("##PT_CLEAN##",ptcfg.getTagId());
//            sql = sqlTemplate.replace("##PTSAVE##",new Integer(ptcfg.getPtSave()).toString());
//            sql = sqlTemplate.replace("##PTTIMEOUT##",new Integer((ptcfg.getPtTimeout())).toString());
//            sql = sqlTemplate.replace("##FORMULA##",ptcfg.getFormula());
//            sql = sqlTemplate.replace("##ORG##",ptcfg.getOrg());
//            sql = sqlTemplate.replace("##SITE##",ptcfg.getSite());
//
//            ps = conn.prepareStatement(sql);
//            ps.executeUpdate();
//        }
//        conn.commit();
//    } catch (ClassNotFoundException e) {
//            log.error("saveConfig failed:",e);
//
//            return false;
//    } catch (SQLException e) {
//            log.error("saveConfig failed:",e);
//            try {
//                conn.rollback();
//            } catch (SQLException e1) {
//                log.error("rollback failed:",e1);
//            }
//            return false;
//        }finally {
//            try {
//                conn.setAutoCommit(true);
//            } catch (SQLException e) {
//                log.error("saveConfig failed:",e);
//                return false;
//            }
//        }
//        return true;
//    }
}
