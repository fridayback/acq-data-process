/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * SaveDataBolt.java
 * Created on  2018/4/2 14:47
 * 版本       修改时间          作者      修改内容
 *            2018/4/2      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.bolt;/*
 * SaveDataBolt
 * Created on  202018/4/2 14:47  by liulin
 *  ${COPYRIGHT}
 */

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.enerbos.dis.dataprocess.util.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

public class SaveDataBolt implements IRichBolt {
    private OutputCollector collector;
    private Logger log = LoggerFactory.getLogger(SaveToDBBolt.class);
    private Map properties;
    private int taskId;
    private KafkaProducer<String,String> kafkaProducer;
    private String disHisTopic;
    //private AcqRuleContainer saveRuleContainer;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        taskId = topologyContext.getThisTaskId();

        String bootstrapServers = map.get("bootstrapServers").toString();
        disHisTopic = map.get("dis-his-topic").toString();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducer = new KafkaProducer<String, String>(props);

    }


    @Override
    public void execute(Tuple tuple) {

        //Object groupId = tuple.getValue(0);
        String dataType = tuple.getValue(1).toString();

        log.trace("-------++++++++ {}:{}",tuple.getValue(1),tuple.getValue(0));
        switch (dataType){
            case DisMessage.MS_RTDATA:
            case DisMessage.MS_HISDATA:{
                ArrayList<PointValue> pvs = (ArrayList<PointValue>) tuple.getValue(0);

                ArrayList datas = new ArrayList<KairosdbData>();
                for (PointValue pv : pvs){
                    KairosdbData kairosdbData = new KairosdbData();
                    kairosdbData.name = pv.getTagid();
                    kairosdbData.timestamp = DateUtil.dateToStamp(pv.getTime());
                    kairosdbData.value = new Double(pv.getPtvalue()).toString();
                    //kairosdbData.status = pv.getStatus();
                    kairosdbData.genTags(pv.getOrg(),pv.getSite(),pv.getStatus());
                    datas.add(kairosdbData);
                }
                try {
                    if (datas.size()>0){
                        ObjectMapper mapper = new ObjectMapper();
                        String json = mapper.writeValueAsString(datas);
                        ProducerRecord<String,String> msg = new ProducerRecord<String,String>(disHisTopic,json);
                        Future<RecordMetadata> res = kafkaProducer.send(msg);
                        log.info("{}",res);
                    }
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                    break;
                }
                break;
            }
//            case DisMessage.MS_ACQCFGCMD:{
//
//                ArrayList<SaveRule> saveRules = (ArrayList<SaveRule>) tuple.getValue(0);
//                for (SaveRule saveRule:saveRules){
//                    saveRuleContainer.save(saveRule.getTagid(),saveRule);
//                }
//
//                break;
//            }
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
