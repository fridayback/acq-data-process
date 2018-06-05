/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * RtDataSpout.java
 * Created on  2018/3/18 9:58
 * 版本       修改时间          作者      修改内容
 *            2018/3/18      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.spout;/*
 * RtDataSpout
 * Created on  202018/3/18 9:58  by liulin
 *  ${COPYRIGHT}
 */

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.enerbos.dis.dataprocess.util.DisMessage;
import com.enerbos.dis.dataprocess.util.GroupingMethod;
import com.enerbos.dis.dataprocess.util.PointValue;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class RtDataSpout implements IRichSpout{
    private SpoutOutputCollector collector;
    private  KafkaConsumer<String, String> consumer;
    private Logger log = LoggerFactory.getLogger(RtDataSpout.class);

    public void ack(Object msgId) {
        log.debug("应答OK:"+msgId);
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
        //log.debug("=========  nexttuple =========={}");
        //////////////   测试代码 模拟kafka取数   //////////////
        boolean debugFlag = false;
        if(debugFlag){
            String s;
            switch (a){
                case 5:
                    a++;
                    s = "{\"MSG_HEAD\":\"begin\",\"MSG_ORG\":\"1111\",\"MSG_PRJ\":\"1122\",\"MSG_SYS\":\"1\",\"MSG_DEV\":\"1\",\"MSG_TYPE\":\"dr_u\",\"MSG_ID\":\"\",\"MSG_SEG\":\"0\",\"MSG_NUM\":\"-1\",\"MSG_BODY\":[{\"ptid\":1,\"ptvalue\":600,\"pttime\":\"2018-02-23 16:44:01\",\"ptclean\":0},{\"ptid\":2,\"ptvalue\":69,\"pttime\":\"2018-02-23 16:44:01\",\"ptclean\":0},{\"ptid\":3,\"ptvalue\":34.24,\"pttime\":\"2018-02-23 16:44:01\",\"ptclean\":0},{\"ptid\":4,\"ptvalue\":22.04,\"pttime\":\"2018-02-23 16:44:01\",\"ptclean\":0},{\"ptid\":5,\"ptvalue\":600,\"pttime\":\"2018-02-23 16:40:00\",\"ptclean\":0},{\"ptid\":6,\"ptvalue\":69,\"pttime\":\"2018-02-23 16:40:00\",\"ptclean\":0},{\"ptid\":7,\"ptvalue\":34.24,\"pttime\":\"2018-02-23 16:40:00\",\"ptclean\":0},{\"ptid\":8,\"ptvalue\":22.04,\"pttime\":\"2018-02-23 16:40:00\",\"ptclean\":0}],\"MSG_TAIL\":\"end\"}";
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
                if( disMessage.getType().equals(DisMessage.MS_RTDATA)){
                    List<PointValue> pointValues = (ArrayList<PointValue>)disMessage.getData();
                    for (PointValue pv:pointValues){
                        this.collector.emit("calculate-data",new Values(pv.getTagid(),pv,disMessage.getType()));
                    }
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
        }else {
            ////////////////////////////////////////////////////////
            try {
                ConsumerRecords<String,String> records = consumer.poll(1000);

                for (final ConsumerRecord<String,String> record : records){
                    String str = record.value();
                    //log.debug("=========  nexttuple  =========\n{}",str.substring(1,250));
                    try{
                        DisMessage disMessage = new DisMessage(str);
                        if(disMessage.getType().equals(DisMessage.MS_RTDATA)){
                            List<PointValue> pointValues = (ArrayList<PointValue>)disMessage.getData();
                            HashMap<Integer,ArrayList<PointValue>> groupMap = new HashMap();
                            for (PointValue pv:pointValues){
                                Integer id = new Integer(GroupingMethod.getGroupIdByKey(pv.getTagid(),GroupingMethod.dataCalculateBoltCount));
                                ArrayList<PointValue> element = groupMap.get(id);
                                if(element == null){
                                    element = new ArrayList<PointValue>();
                                    groupMap.put(id,element);
                                }
                                element.add(pv);
                            }
                            Iterator iter = groupMap.entrySet().iterator();
                            while (iter.hasNext()){
                                Map.Entry entry = (Map.Entry) iter.next();
                                this.collector.emit("calculate-data",new Values(entry.getKey(),entry.getValue(),disMessage.getType()));
                            }
                        }
                    }catch (Exception e){
                        e.printStackTrace();
                    }


                }
            }catch (Exception e){
                throw new RuntimeException("Error consumer tuple",e);
            }
        }

    }
    /**
     * 我们将创建一个文件并维持一个collector对象
     */
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        String bootstrapServers = conf.get("bootstrapServers").toString();
        String topicstr = conf.get("rt-data-topic").toString();
        String groupId = conf.get("rt-data-groupid").toString();

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
        //declarer.declare(new Fields("origin-data"));
        //按数据类型进行分组：历史数据，实时数据
        declarer.declareStream("calculate-data",new Fields("tagid","data","type"));
    }

    public Map<String, Object> getComponentConfiguration() {
        log.info("=========  getComponentConfiguration ==========");
        return null;
    }
}
