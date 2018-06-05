/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * SaveRealTimeDataBolt.java
 * Created on  2018/1/17 13:51
 * 版本       修改时间          作者      修改内容
 *            2018/1/17      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.bolt;/*
 * SaveRealTimeDataBolt
 * Created on  202018/1/17 13:51  by liulin
 *  ${COPYRIGHT}
 */

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.enerbos.dis.dataprocess.util.AcqRuleContainer;
import com.enerbos.dis.dataprocess.util.DataTimeOutRule;
import com.enerbos.dis.dataprocess.util.DisMessage;
import com.enerbos.dis.dataprocess.util.PointValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.ArrayList;
import java.util.Map;

public class SaveRealTimeDataBolt implements IRichBolt {
    private OutputCollector collector;
    private Logger log = LoggerFactory.getLogger(SaveRealTimeDataBolt.class);
    private Map properties;
    private int taskId;
    private Jedis jedis;
    private AcqRuleContainer timeoutRuleContainer;
    public static final String TAG_ACQ_PREFIX = "ACQDATA_R_";
    public static final String TAG_ON_OFF_PREFIX = TAG_ACQ_PREFIX+"ST_";
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        taskId = topologyContext.getThisTaskId();
        String redisHost = map.get("redis-host").toString();
        String redisHostArray[] = redisHost.split(":");
        try {
            timeoutRuleContainer = new AcqRuleContainer(map,DataTimeOutRule.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        jedis = new Jedis(redisHostArray[0],new Integer(redisHostArray[1]).intValue());
        //jedis.auth("password");
        //jedis.connect();
    }


    @Override
    public void execute(Tuple tuple) {

        Object groupId = tuple.getValue(0);
        String dataType = tuple.getValue(2).toString();

        log.trace("-------++++++++ {}:{}",tuple.getValue(2),tuple.getValue(1));
        switch (dataType){
            case DisMessage.MS_RTDATA:{
                ArrayList<PointValue> pvs = (ArrayList<PointValue>) tuple.getValue(1);
                Pipeline pipeline = jedis.pipelined();
                String time = "--";
                for (PointValue pv :pvs){
                    DataTimeOutRule dataTimeOutRule = (DataTimeOutRule) timeoutRuleContainer.getRule(pv.getTagid());
                    int timeout = 632448000;//>20年
                    if (null != dataTimeOutRule || dataTimeOutRule.getTimeout() > 0){
                        timeout = dataTimeOutRule.getTimeout();
                    }
                    try {
                        pipeline.set(TAG_ACQ_PREFIX + pv.getTagid(), pv.getTagid() + ",0," + pv.getTime() + "," + new Double(pv.getPtvalue()).toString() + "," + new Integer(pv.getStatus()).toString());
                        pipeline.setex(TAG_ON_OFF_PREFIX + pv.getTagid(), timeout, "");
                        log.trace("---------------------- 存储数据到redis:{}", pv.toString());
                    }catch (Exception e){
                        log.error("save acq data to redis failed:{}:{}",pv,e);
                    }
                    time = pv.getTime();
                }
                //pipeline.setex("test", 1, time);
                pipeline.sync();
                break;
            }
            case DisMessage.MS_ACQCFGCMD:{

                ArrayList<DataTimeOutRule> dataTimeOutRules = (ArrayList<DataTimeOutRule>) tuple.getValue(1);
                for (DataTimeOutRule dataTimeOutRule: dataTimeOutRules){
                    timeoutRuleContainer.save(dataTimeOutRule.getTagid(),dataTimeOutRule);
                }

                break;
            }
            default:
        }
        collector.ack(tuple);

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("history-data", new Fields("site"));
        outputFieldsDeclarer.declareStream("real-time-data", new Fields("site"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
