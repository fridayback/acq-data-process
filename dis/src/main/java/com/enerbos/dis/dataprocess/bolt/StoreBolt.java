/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * StoreBolt.java
 * Created on  2018/3/20 15:46
 * 版本       修改时间          作者      修改内容
 *            2018/3/20      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.bolt;/*
 * StoreBolt
 * Created on  202018/3/20 15:46  by liulin
 *  ${COPYRIGHT}
 */

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.enerbos.dis.dataprocess.util.KairosdbData;
import com.enerbos.dis.dataprocess.util.KairosdbOperator;
import com.enerbos.dis.dataprocess.util.PointValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StoreBolt implements IRichBolt {
    private OutputCollector collector;
    private int taskId;
    private KairosdbOperator kairosdbOperator;
    private Logger log = LoggerFactory.getLogger(StoreBolt.class);
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        taskId = topologyContext.getThisTaskId();
        String kairosdbHost = map.get("kairosdb-host").toString();
        kairosdbOperator = new KairosdbOperator(kairosdbHost);
    }

    private KairosdbData genKairosdbDataR(KairosdbData data){
        int status = new Integer(data.tags.get("status").toString()).intValue();
        if(status != PointValue.STATUS_OK) return null;
        KairosdbData dataR = new KairosdbData();
        dataR.name = data.name;
        dataR.timestamp = data.timestamp;
        dataR.value = data.value;
        dataR.tags = new HashMap();
        dataR.tags.putAll(data.tags);
        dataR.tags.remove("status");
        dataR.tags.put("rstatus",new Integer(PointValue.STATUS_OK).toString());
        return dataR;
    }
    @Override
    public void execute(Tuple tuple) {

        ArrayList<KairosdbData> datas = (ArrayList<KairosdbData>) tuple.getValue(0);
        List datasAll = new ArrayList<>();
        for (KairosdbData d:datas){
            KairosdbData dataR = genKairosdbDataR(d);
            if(null != dataR){
                datasAll.add(dataR);
            }
        }
        datasAll.addAll(datas);

        if(kairosdbOperator.saveDatas(datasAll)){
            collector.ack(tuple);
        }
        else{
            collector.fail(tuple);
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("store-data", new Fields("data"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
