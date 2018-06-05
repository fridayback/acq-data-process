/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * CmdDataSpout.java
 * Created on  2018/1/16 10:08
 * 版本       修改时间          作者      修改内容
 *            2018/1/16      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.spout;/*
 * CmdDataSpout
 * Created on  202018/1/16 10:08  by liulin
 *  ${COPYRIGHT}
 */

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import com.enerbos.dis.dataprocess.util.DisMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.topology.IRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class CmdDataSpout implements IRichSpout {
    private SpoutOutputCollector collector;
    private  KafkaConsumer<String, String> consumer;
    private Logger log = LoggerFactory.getLogger(CmdDataSpout.class);

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
        boolean debugFlag = false;
        if(debugFlag){
            String s;
            switch (a){
                case 0:
                    a++;
                    s = "{\"MSG_HEAD\":\"begin\",\"MSG_ORG\":\"1111\",\"MSG_PRJ\":\"1122\",\"MSG_SYS\":\"\",\"MSG_DEV\":\"1\",\"MSG_TYPE\":\"conf_d\",\"MSG_ID\":\"20180316-12:30:46\",\"MSG_SEG\":\"0\",\"MSG_NUM\":\"-1\",\"MSG_BODY\":{\"drvlist\":[{\"drvname\":\"sensor\",\"drvid\":\"1\"}],\"subsyslist\":[{\"subsysid\":\"1\",\"subsysparam\":\"sensor\",\"subsystype\":\"\",\"terlist\":[{\"terid\":1,\"drvid\":\"1\",\"terparam\":\"sensor\",\"points\":[{\"ptid\":2,\"ptname\":\"2\",\"ptparam\":\"10800117C63143C8_battery\",\"ptclean\":\"\",\"ptsave\":1,\"formula\":\"\",\"pttimeout\":0,\"pt-collect-en\":1},{\"ptid\":1,\"ptname\":\"1\",\"ptparam\":\"10800117C63143C8_temperature\",\"ptclean\":\"[1,2]\",\"ptsave\":1,\"formula\":\"1000\",\"pttimeout\":0,\"pt-collect-en\":1},{\"ptid\":4,\"ptname\":\"4\",\"ptparam\":\"10800117C674C217_battery\",\"ptclean\":\"\",\"ptsave\":1,\"formula\":\"\",\"pttimeout\":0,\"pt-collect-en\":1},{\"ptid\":3,\"ptname\":\"3\",\"ptparam\":\"10800117C674C217_temperature\",\"ptclean\":\"\",\"ptsave\":1,\"formula\":\"\",\"pttimeout\":0,\"pt-collect-en\":1},{\"ptid\":6,\"ptname\":\"6\",\"ptparam\":\"10B10117C601B2C0_battery\",\"ptclean\":\"\",\"ptsave\":1,\"formula\":\"\",\"pttimeout\":0,\"pt-collect-en\":1},{\"ptid\":5,\"ptname\":\"5\",\"ptparam\":\"10B10117C601B2C0_temperature\",\"ptclean\":\"\",\"ptsave\":1,\"formula\":\"\",\"pttimeout\":0,\"pt-collect-en\":1}]}]}]},\"MSG_TAIL\":\"end\"}";
                    break;
                default:{
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    a++;
                    return;
                }
            }
            try {
                DisMessage disMessage = new DisMessage(s);
                if(disMessage.getType().equals(DisMessage.MS_ACQCFGCMD)){
                    this.collector.emit("acq-config",new Values(disMessage.getData()));
                }
            }catch (NullPointerException e) {
                log.error("error dis message:",e);
                return;
            }
            catch (IOException e){
                log.error("error dis message:",e);
                return;
            }
            return;
        }
        ////////////////////////////////////////////////////////
        try {
            ConsumerRecords<String,String> records = consumer.poll(1000);

            for (final ConsumerRecord<String,String> record : records){
                String str = record.value();
                log.debug("=========  nexttuple ==========\n{}",str);
                try{
                    DisMessage disMessage = new DisMessage(str);
                    if(disMessage.getType().equals(DisMessage.MS_ACQCFGCMD)){
                        this.collector.emit("acq-config",new Values(disMessage.getData()));
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }


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
        String topicstr = conf.get("cmd-topic").toString();
        String groupId = conf.get("cmd-groupid").toString();

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
        declarer.declareStream("acq-config",new Fields("config"));
    }

    public Map<String, Object> getComponentConfiguration() {
        log.info("=========  getComponentConfiguration ==========");
        return null;
    }
}
