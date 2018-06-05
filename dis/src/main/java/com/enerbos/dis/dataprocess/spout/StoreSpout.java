/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * StoreSpout.java
 * Created on  2018/3/20 15:10
 * 版本       修改时间          作者      修改内容
 *            2018/3/20      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.spout;/*
 * StoreSpout
 * Created on  202018/3/20 15:10  by liulin
 *  ${COPYRIGHT}
 */

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.enerbos.dis.dataprocess.util.KairosdbData;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class StoreSpout implements IRichSpout {
    private SpoutOutputCollector collector;
    private KafkaConsumer<String, String> consumer;
    private Logger log = LoggerFactory.getLogger(StoreSpout.class);

    public void ack(Object msgId) {
        log.info("应答OK:"+msgId);
    }
    public void close() {
        log.info("close spout");
        consumer.close();
    }

    public void activate() {
        log.debug("=========  activate ==========");
    }

    public void deactivate() {
        log.debug("=========  deactivate ==========");
    }
    static int a = 0;
    public void fail(Object msgId) {
        log.error("FAIL:"+msgId);
    }
    /**
     * 这个方法做的惟一一件事情就是分发文件中的文本行
     */
    public void nextTuple() {
        /**
         * 这个方法会不断的被调用，直到整个文件都读完了，我们将等待并返回。
         */
        //log.debug("=========  nexttuple ==========");
        //////////////   测试代码 模拟kafka取数   //////////////
        try {
            ConsumerRecords<String,String> records = consumer.poll(1000);

            ArrayList<KairosdbData> datas = new ArrayList();
            for (final ConsumerRecord<String,String> record : records){
                String str = record.value();
                ObjectMapper mapper = new ObjectMapper();
                try{
                    ArrayList<KairosdbData> kairosdbDatas = (ArrayList<KairosdbData>)mapper.readValue(str, new TypeReference<ArrayList<KairosdbData>>(){});
                    datas.addAll(kairosdbDatas);
                } catch (JsonParseException e){
                    log.error("{}",e);
                } catch (JsonMappingException e){
                    log.error("{}",e);
                }catch (IOException e){
                    log.error("{}",e);
                }

            }
            if(datas.size()>0){
                collector.emit("store-data",new Values(datas));
            }
        }catch (Exception e){
            throw new RuntimeException("Error consumer tuple",e);
        }

    }
    /**
     * 我们将创建一个文件并维持一个collector对象
     */
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        String bootstrapServers = conf.get("bootstrapServers").toString();
        String topicstr = conf.get("dis-his-topic").toString();
        String groupId = conf.get("dis-his-groupid").toString();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        this.consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topicstr.split(",")));

        this.collector = collector;
    }
    /**
     * 声明输入域"word"
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        log.info("=========  declareOutputFields ==========");
        declarer.declareStream("store-data",new Fields("data"));
    }

    public Map<String, Object> getComponentConfiguration() {
        log.info("=========  getComponentConfiguration ==========");
        return null;
    }
}
