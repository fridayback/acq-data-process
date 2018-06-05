/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * CalculateBolt.java
 * Created on  2018/3/13 10:17
 * 版本       修改时间          作者      修改内容
 *            2018/3/13      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.bolt;/*
 * CalculateBolt
 * Created on  202018/3/13 10:17  by liulin
 *  ${COPYRIGHT}
 */

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.enerbos.dis.dataprocess.util.AcqRuleContainer;
import com.enerbos.dis.dataprocess.util.CalculateRule;
import com.enerbos.dis.dataprocess.util.DisMessage;
import com.enerbos.dis.dataprocess.util.PointValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;

public class CalculateBolt implements IRichBolt {
    private OutputCollector collector;
    private int taskId;
    private AcqRuleContainer calculateRuleContainer;
    private Logger log = LoggerFactory.getLogger(CalculateBolt.class);
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        taskId = topologyContext.getThisTaskId();
        try {
            calculateRuleContainer = new AcqRuleContainer(map,CalculateRule.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {

        //String tagid = tuple.getValue(0).toString();
        String dataType = tuple.getValue(2).toString();

        log.trace("-------++++++++ {}:{}",tuple.getValue(2),tuple.getValue(1));
        //String streamId = tuple.getSourceStreamId();
        switch (dataType){
            case DisMessage.MS_RTDATA:
            case DisMessage.MS_HISDATA:{
                ArrayList<PointValue> pvs = (ArrayList<PointValue>) tuple.getValue(1);
                for (PointValue pv :pvs){
                    CalculateRule calculateRule = (CalculateRule) calculateRuleContainer.getRule(pv.getTagid());
                    if(null != calculateRule){
                        pv = calculateRule.getCalaulteResult(pv);
                    }
                }
                collector.emit("clean-data",new Values(tuple.getValue(0),pvs,dataType));

                collector.ack(tuple);
                break;
            }
            case DisMessage.MS_ACQCFGCMD:{
                try {

                    ArrayList<CalculateRule> calculateRules = (ArrayList<CalculateRule>) tuple.getValue(1);
                    for (CalculateRule calculateRule: calculateRules){
                        calculateRuleContainer.save(calculateRule.getTagid(),calculateRule);
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
        outputFieldsDeclarer.declareStream("clean-data", new Fields("tagid","data","type"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
