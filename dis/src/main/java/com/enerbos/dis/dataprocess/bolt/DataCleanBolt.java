/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * DataClean.java
 * Created on  2018/1/5 13:39
 * 版本       修改时间          作者      修改内容
 *            2018/1/5      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.bolt;/*
 * DataClean
 * Created on  202018/1/5 13:39  by liulin
 *  ${COPYRIGHT}
 */


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.enerbos.dis.dataprocess.util.AcqRuleContainer;
import com.enerbos.dis.dataprocess.util.CleanRule;
import com.enerbos.dis.dataprocess.util.DisMessage;
import com.enerbos.dis.dataprocess.util.PointValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;


public class DataCleanBolt implements IRichBolt {

    private OutputCollector collector;
    private int taskId;
    private AcqRuleContainer cleanRuleContainer;
    private Logger log = LoggerFactory.getLogger(DataCleanBolt.class);
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        taskId = topologyContext.getThisTaskId();
        try {
            cleanRuleContainer = new AcqRuleContainer(map,CleanRule.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {

        Object groupId = tuple.getValue(0);
        String messageType = tuple.getValue(2).toString();

        log.trace("-------++++++++ {}:{}",tuple.getValue(2),tuple.getValue(1));
        switch (messageType){
            case DisMessage.MS_RTDATA:{
                try {
                    ArrayList<PointValue> pvs = (ArrayList<PointValue>) tuple.getValue(1);
                    for (PointValue pv: pvs){
                        CleanRule cleanRule = (CleanRule) cleanRuleContainer.getRule(pv.getTagid());
                        pv = cleanRule.clean(pv);
                    }
                    collector.emit("timeout-data",new Values(groupId,pvs,messageType));
                    collector.emit("save-data",new Values(groupId,pvs,messageType));
                    collector.ack(tuple);
                }catch (NullPointerException e) {
                    e.printStackTrace();
                    collector.ack(tuple);
                } catch (Exception e){
                    e.printStackTrace();
                    collector.ack(tuple);
                }
                break;
            }
            case DisMessage.MS_HISDATA:{
                try {
                    ArrayList<PointValue> pvs = (ArrayList<PointValue>) tuple.getValue(1);
                    for (PointValue pv:pvs){
                        CleanRule cleanRule = (CleanRule) cleanRuleContainer.getRule(pv.getTagid());
                        pv = cleanRule.clean(pv);
                    }
                    collector.emit("save-data",new Values(groupId,pvs,messageType));
                    collector.ack(tuple);
                    break;
                }catch (NullPointerException e) {
                    e.printStackTrace();
                    collector.ack(tuple);
                } catch (Exception e){
                    e.printStackTrace();
                    collector.ack(tuple);
                }
                break;
            }
            case DisMessage.MS_ACQCFGCMD:{

                ArrayList<CleanRule> cleanRules = (ArrayList<CleanRule>) tuple.getValue(1);
                for (CleanRule cleanRule:cleanRules){
                    cleanRuleContainer.save(cleanRule.getTagid(),cleanRule);
                }

                collector.ack(tuple);
            }
            default:collector.ack(tuple);
        }

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("save-data", new Fields("tagid","data","type"));
        outputFieldsDeclarer.declareStream("timeout-data", new Fields("tagid","data","type"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
