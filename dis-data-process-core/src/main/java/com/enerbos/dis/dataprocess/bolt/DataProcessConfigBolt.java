/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * DataProcessConfigBolt.java
 * Created on  2018/4/2 14:10
 * 版本       修改时间          作者      修改内容
 *            2018/4/2      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.bolt;/*
 * DataProcessConfigBolt
 * Created on  202018/4/2 14:10  by liulin
 *  ${COPYRIGHT}
 */

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.enerbos.dis.dataprocess.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

import java.util.*;

public class DataProcessConfigBolt implements IRichBolt {
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
        outputFieldsDeclarer.declareStream("calculate-data",new Fields("tagid","data","type"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private boolean updateAcqConfig(List cfgs){

        List<PointCfg> pointConfigList = cfgs;

        HashMap<Integer,ArrayList> dataProcessRuleMap = new HashMap<>();

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
            Integer groupId = new Integer(GroupingMethod.getGroupIdByKey(p.getTagId(),GroupingMethod.dataProcessBoltCount));
            DataProcessRule dataProcessRule = new DataProcessRule(calculateRule,cleanRule,dataTimeOutRule,saveRule);
            ArrayList rs = dataProcessRuleMap.get(groupId);
            if(rs == null){
                rs = new ArrayList();
                dataProcessRuleMap.put(groupId,rs);
            }
            rs.add(dataProcessRule);

        }

        Iterator itorDataProcessRule = dataProcessRuleMap.entrySet().iterator();
        while(itorDataProcessRule.hasNext()){
            Map.Entry entry = (Map.Entry) itorDataProcessRule.next();
            collector.emit("calculate-data",new Values(entry.getKey(),entry.getValue(),DisMessage.MS_ACQCFGCMD));
        }

        return true;
    }
}
