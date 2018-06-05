/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * DataProcessBolt.java
 * Created on  2018/3/27 14:23
 * 版本       修改时间          作者      修改内容
 *            2018/3/27      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.bolt;/*
 * DataProcessBolt
 * Created on  202018/3/27 14:23  by liulin
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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.ArrayList;
import java.util.Map;

public class DataProcessBolt implements IRichBolt {
    private OutputCollector collector;
    private int taskId;
    private DataProcessRuleContainer dataProcessRuleContainer;

    private Logger log = LoggerFactory.getLogger(DataProcessBolt.class);
    private Jedis jedis;
    private KafkaProducer<String,String> kafkaProducer;
    private String disHisTopic;

    public static final String TAG_ACQ_PREFIX = "ACQDATA_R_";
    public static final String TAG_ON_OFF_PREFIX = TAG_ACQ_PREFIX+"ST_";
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        taskId = topologyContext.getThisTaskId();
        try {
            dataProcessRuleContainer = new DataProcessRuleContainer(map);
        } catch (Exception e) {
            e.printStackTrace();
        }
        String redisHost = map.get("redis-host").toString();
        String redisHostArray[] = redisHost.split(":");

        jedis = new Jedis(redisHostArray[0],new Integer(redisHostArray[1]).intValue());
        //jedis.auth("password");
        //jedis.connect();
    }

    @Override
    public void execute(Tuple tuple) {

        //String groupId = tuple.getValue(0).toString();
        String dataType = tuple.getValue(2).toString();

        log.trace("-------++++++++ {}:{}",tuple.getValue(2),tuple.getValue(1));
        //String streamId = tuple.getSourceStreamId();
        String timestr ="";
        switch (dataType){
            case DisMessage.MS_RTDATA:{
                ArrayList<PointValue> pvs = (ArrayList<PointValue>) tuple.getValue(1);
                ArrayList<PointValue> savePvs = new ArrayList<>();
                Pipeline pipeline = jedis.pipelined();
                for (PointValue pv :pvs){
                    timestr = pv.getTime();
                    DataProcessRule r = dataProcessRuleContainer.getRule(pv.getTagid());
                    if(r == null) continue;
                    CalculateRule calculateRule = r.getCalculateRule();
                    if(null == calculateRule){
                        continue;
                    }
                    pv = calculateRule.getCalaulteResult(pv);

                    CleanRule cleanRule = r.getCleanRule();
                    if(null == cleanRule) continue;
                    pv = cleanRule.clean(pv);

                    DataTimeOutRule dataTimeOutRule = r.getDataTimeOutRule();
                    if(null == dataTimeOutRule) continue;
                    int timeout = 632448000;//>20年
                    if (dataTimeOutRule.getTimeout() > 0){
                        timeout = dataTimeOutRule.getTimeout();
                    }
                    try {
                        pipeline.set(TAG_ACQ_PREFIX + pv.getTagid(), pv.getTagid() + ",0," + pv.getTime() + "," + new Double(pv.getPtvalue()).toString() + "," + new Integer(pv.getStatus()).toString());
                        pipeline.setex(TAG_ON_OFF_PREFIX + pv.getTagid(), timeout, "");
                        log.trace("---------------------- 存储数据到redis:{}", pv.toString());
                    }catch (Exception e){
                        log.error("save acq data to redis failed:{}:{}",pv,e);
                    }
                    SaveRule saveRule = r.getSaveRule();
                    if(null == saveRule) continue;
                    PointValue pvsave = saveRule.getSavaData(pv);
                    if(null != pvsave) savePvs.add(pvsave);
                    pv.setOrg(saveRule.getOrg());
                    pv.setSite(saveRule.getSite());
                }
                pipeline.sync();

                if(savePvs.size()>0){
                    collector.emit("save-data",new Values(savePvs,dataType));
                }
                //log.debug("-----------{}",timestr);
                collector.ack(tuple);
                break;
            }
            case DisMessage.MS_HISDATA:{
                ArrayList<PointValue> pvs = (ArrayList<PointValue>) tuple.getValue(1);
                ArrayList<PointValue> savePvs = new ArrayList<>();
                for (PointValue pv :pvs){
                    DataProcessRule r = dataProcessRuleContainer.getRule(pv.getTagid());
                    CalculateRule calculateRule = r.getCalculateRule();
                    if(null == calculateRule){
                        continue;
                    }
                    pv = calculateRule.getCalaulteResult(pv);

                    CleanRule cleanRule = r.getCleanRule();
                    if(null == cleanRule) continue;
                    pv = cleanRule.clean(pv);
                    pv.setOrg(cleanRule.getOrg());
                    pv.setSite(cleanRule.getSite());
                    savePvs.add(pv);
                }
                if(savePvs.size()>0){
                    collector.emit("save-data",new Values(savePvs,dataType));
                }

                collector.ack(tuple);
                break;
            }
            case DisMessage.MS_ACQCFGCMD:{
                try {

                    ArrayList<DataProcessRule> rs = (ArrayList<DataProcessRule>) tuple.getValue(1);
                    for (DataProcessRule r: rs){
                        dataProcessRuleContainer.save(r.getCalculateRule().getTagid(),r);
                    }
                    collector.ack(tuple);
                    break;
                } catch (NullPointerException e) {
                    e.printStackTrace();
                    collector.ack(tuple);
                    return;
                }catch (Exception e){
                    e.printStackTrace();
                    collector.ack(tuple);
                    return;
                }

            }
            default:collector.ack(tuple);
        }

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("save-data", new Fields("data","type"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
