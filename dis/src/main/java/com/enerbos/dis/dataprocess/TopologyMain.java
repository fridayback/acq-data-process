/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * TopologyMain.java
 * Created on  2018/1/5 10:30
 * 版本       修改时间          作者      修改内容
 *            2018/1/5      liulin    初始版本
 */
package com.enerbos.dis.dataprocess;/*
 * TopologyMain
 * Created on  202018/1/5 10:30  by liulin
 *  ${COPYRIGHT}
 */

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.enerbos.dis.dataprocess.bolt.*;
import com.enerbos.dis.dataprocess.spout.CmdDataSpout;
import com.enerbos.dis.dataprocess.spout.HisDataSpout;
import com.enerbos.dis.dataprocess.spout.RtDataSpout;
import com.enerbos.dis.dataprocess.spout.StoreSpout;
import com.enerbos.dis.dataprocess.util.GroupingMethod;
import com.enerbos.dis.dataprocess.util.KairosdbData;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.IOException;
import java.sql.*;
import java.util.*;


public class TopologyMain {
    private String dbHost;
    private String dbName;
    private String dbUser;
    private String dbPassword;

    private static Logger log = LoggerFactory.getLogger(TopologyMain.class);

    private static final String VER = "dis-data-stream-process_v0.1";



    static boolean loadAcqConfig(String dbHost,String dbName,String dbUser,String dbPassword,String redisHost){

        String url = "jdbc:mysql://"+dbHost+"/"+dbName;
        HashMap<String,Map> newRules = new HashMap();
        try {
            Connection conn = DriverManager.getConnection(url, dbUser, dbPassword);

            if(conn.isClosed()){
                log.error("config synchronize failed: connect to db:{} is closed",dbHost);
                return false;
            }

            String sqlQuery = "SELECT tagid,pt_clean,formula,pt_save,pt_timeout,point.org AS org,point.site AS site FROM point INNER JOIN collector ON point.c_id = collector.c_id WHERE collector.`modify` = 0 AND point.pub <> 'g' " +
                    "UNION SELECT tagid,pt_clean,formula,pt_save,pt_timeout,\"puborg\",\"pubsite\" FROM point_public INNER JOIN collector ON point_public.c_id = collector.c_id WHERE collector.`modify` = 0";
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sqlQuery);

            while(rs.next()){
                String cleanRule = rs.getString("pt_clean");
                switch (cleanRule){
                    case "null": {
                        cleanRule = "";
                        break;
                    }
                    default:
                }
                String calculateRule = rs.getString("formula");
                switch (calculateRule){
                    case "null": {
                        calculateRule = "1";
                        break;
                    }
                    default:
                }
                String saveRule = rs.getString("pt_save");
                switch (saveRule){
                    case "null": {
                        saveRule = "0";
                        break;
                    }
                    default:
                }
                String timeoutRule = rs.getString("pt_timeout");
                switch (timeoutRule){
                    case "null": {
                        timeoutRule = "-1";
                        break;
                    }
                    default:
                }
                String org = rs.getString("org");
                String site = rs.getString("site");
                Map<String,String> rules = new HashMap<String,String>();
                rules.put(ConfigBolt.ORG,org);
                rules.put(ConfigBolt.SITE,site);
                rules.put(ConfigBolt.CLEAN_RULE,cleanRule);
                rules.put(ConfigBolt.CALCULATE_RULE,calculateRule);
                rules.put(ConfigBolt.SAVE_RULE,saveRule);
                rules.put(ConfigBolt.TIMEOUT_RULE,timeoutRule);
                newRules.put(rs.getString("tagid"), rules);
            }
            rs.close();
            stmt.close();
            conn.close();

        }catch(SQLException e) {
            log.error("",e);
            return false;
        } catch(Exception e) {
            log.error("",e);
            return false;
        }

        String [] redisParam = redisHost.split(":");
        Jedis jedis = new Jedis(redisParam[0],new Integer(redisParam[1]).intValue());
        if(!jedis.isConnected()) jedis.connect();
        Pipeline pipeline = jedis.pipelined();
        //jedis.auth("admin");验证密码
        //////////////////////////////////////////////////////////
        for (Map.Entry<String, Map> entry:newRules.entrySet()){
            //if(!jedis.isConnected()) jedis.connect();
            pipeline.hmset(entry.getKey(),entry.getValue());
        }
        //List results = pipeline.syncAndReturnAll();
        //log.info("set acq config to redis :{}",results);
        return true;
    }
    static Properties loadDefaultProperties(){
        Properties properties = new Properties();
        properties.put("bootstrapServers","192.168.1.9:9092");
        properties.put("rt-data-groupid","rt_data_group");
        properties.put("rt-data-topic","RT_DATA");
        properties.put("his-data-groupid","his_data_group");
        properties.put("his-data-topic","HIS_DATA");//采集原始历史数据通道
        properties.put("dis-his-topic","DIS_HIS");//清洗历史数据通道
        properties.put("dis-his-groupid","dis_his_group");
        properties.put("cmd-groupid","cmd_group");
        properties.put("cmd-topic","KFTP_CD");
        properties.put("redis-host","192.168.1.9:6379");
        properties.put("db-host","192.168.1.9:3306");
        properties.put("db-name","huangjing_test");
        properties.put("db-user","root");
        properties.put("db-password","enerbos,123");
        properties.put("kairosdb-host","192.168.1.176:8080");

        properties.put("cmd-spout-count","1");
        properties.put("acq-config-bolt-count","1");
        properties.put("his-spout-count","1");
        properties.put("rt-spout-count","2");
        properties.put("calculate-bolt-count","2");
        properties.put("clean-bolt-count","2");
        properties.put("rt-bolt-count","2");
        properties.put("his-bolt-count","1");
        properties.put("store-spout-count","1");
        properties.put("store-bolt-count","1");
        properties.put("data-process-bolt-count","1");
        return properties;
    }
    static Config loadProperties(){
        Config conf = new Config();
        Properties properties = new Properties(loadDefaultProperties());
        // 读取src下配置文件 在resource目录下--- 属于读取内部文件 注意："/" 必须有，是指根本下
        try {
            properties.load(TopologyMain.class.getResourceAsStream("/sysconf.properties"));
            conf.put("bootstrapServers",properties.getProperty("bootstrapServers"));
            conf.put("rt-data-groupid",properties.getProperty("rt-data-groupid"));
            conf.put("rt-data-topic",properties.getProperty("rt-data-topic"));
            conf.put("his-data-groupid",properties.getProperty("his-data-groupid"));
            conf.put("his-data-topic",properties.getProperty("his-data-topic"));//采集原始历史数据通道
            conf.put("dis-his-topic",properties.getProperty("dis-his-topic"));//清洗历史数据通道
            conf.put("dis-his-groupid",properties.getProperty("dis-his-groupid"));//清洗历史数据通道
            conf.put("cmd-groupid",properties.getProperty("cmd-groupid"));
            conf.put("cmd-topic",properties.getProperty("cmd-topic"));
            conf.put("redis-host",properties.getProperty("redis-host"));
            conf.put("db-host",properties.getProperty("db-host"));
            conf.put("db-name",properties.getProperty("db-name"));
            conf.put("db-user",properties.getProperty("db-user"));
            conf.put("db-password",properties.getProperty("db-password"));
            conf.put("kairosdb-host",properties.getProperty("kairosdb-host"));
            ///////////////////////////////////////
            GroupingMethod.cmdMessageReaderSpoutCount = new Integer(properties.getProperty("cmd-spout-count").toString()).intValue();
            GroupingMethod.acqConfigBoltCount = new Integer(properties.getProperty("acq-config-bolt-count").toString()).intValue();
            GroupingMethod.hisdataMessageReaderSpoutCount = new Integer(properties.getProperty("his-spout-count").toString()).intValue();
            GroupingMethod.rtdataMessageReaderSpoutCount = new Integer(properties.getProperty("rt-spout-count").toString()).intValue();
            GroupingMethod.dataCalculateBoltCount = new Integer(properties.getProperty("calculate-bolt-count").toString()).intValue();
            GroupingMethod.dataCleanBoltCount = new Integer(properties.getProperty("clean-bolt-count").toString()).intValue();
            GroupingMethod.rtdataSaveBoltCount = new Integer(properties.getProperty("rt-bolt-count").toString()).intValue();
            GroupingMethod.hisdataSaveBoltCount = new Integer(properties.getProperty("his-bolt-count").toString()).intValue();
            GroupingMethod.storeDataMessageReaderSpoutCount = new Integer(properties.getProperty("store-spout-count").toString()).intValue();
            GroupingMethod.storeDataBoltCount = new Integer(properties.getProperty("store-bolt-count").toString()).intValue();
            GroupingMethod.dataProcessBoltCount = new Integer(properties.getProperty("data-process-bolt-count").toString()).intValue();
        } catch (IOException e) {
            log.error(e.getMessage());



        }
        return conf;

    }
    static void test() {
        String str = "[{\"name\":\"5\",\"timestamp\":1519375200000,\"value\":\"600.0\",\"tags\":{\"sec\":\"00\",\"site\":\"1122\",\"min\":\"40\",\"week\":\"05\",\"hour\":\"16\",\"year\":\"18\",\"org\":\"1111\",\"mon\":\"02\",\"day\":\"23\"}}]";
        ObjectMapper mapper = new ObjectMapper();
        try {
            ArrayList<KairosdbData> kairosdbData = mapper.readValue(str, ArrayList.class);
            log.info("{}",kairosdbData);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    static void topology1(TopologyBuilder builder){
        builder.setSpout("cmd-message-reader-spout",new CmdDataSpout(), GroupingMethod.cmdMessageReaderSpoutCount);
        builder.setBolt("acq-config-bolt",new ConfigBolt(),GroupingMethod.acqConfigBoltCount).localOrShuffleGrouping("cmd-message-reader-spout","acq-config");

        builder.setSpout("hisdata-message-reader-spout",new HisDataSpout(),GroupingMethod.hisdataMessageReaderSpoutCount);
        builder.setSpout("rtdata-message-reader-spout",new RtDataSpout(),GroupingMethod.rtdataMessageReaderSpoutCount);
        ///////////////////////////////////////////////////////////////////////////////////////////
        builder.setBolt("data-calculate-bolt",new CalculateBolt() ,GroupingMethod.dataCalculateBoltCount)
                .customGrouping("rtdata-message-reader-spout","calculate-data",new TagIdGrouping())
                .customGrouping("hisdata-message-reader-spout","calculate-data",new TagIdGrouping())
                .customGrouping("acq-config-bolt","calculate-config",new TagIdGrouping());


        builder.setBolt("data-clean-bolt",new DataCleanBolt() ,GroupingMethod.dataCleanBoltCount)
                .customGrouping("data-calculate-bolt","clean-data",new TagIdGrouping())
                .customGrouping("acq-config-bolt","clean-config",new TagIdGrouping());

        builder.setBolt("real-data-save-bolt", new SaveRealTimeDataBolt(),GroupingMethod.rtdataSaveBoltCount)
                .customGrouping("data-clean-bolt","timeout-data", new TagIdGrouping())
                .customGrouping("acq-config-bolt","timeout-config", new TagIdGrouping());

        builder.setBolt("his-data-save-bolt", new SaveToDBBolt(),GroupingMethod.hisdataSaveBoltCount)
                .customGrouping("data-clean-bolt","save-data", new TagIdGrouping())
                .customGrouping("acq-config-bolt","save-config", new TagIdGrouping());

        builder.setSpout("store-data-message-reader-spout",new StoreSpout(),GroupingMethod.storeDataMessageReaderSpoutCount);
        builder.setBolt("store-data-bolt",new StoreBolt(),GroupingMethod.storeDataBoltCount)
                .localOrShuffleGrouping("store-data-message-reader-spout","store-data");
    }
    static void topology2(TopologyBuilder builder){
        builder.setSpout("cmd-message-reader-spout",new CmdDataSpout(), GroupingMethod.cmdMessageReaderSpoutCount);
        builder.setBolt("acq-config-bolt",new DataProcessConfigBolt(),GroupingMethod.acqConfigBoltCount).localOrShuffleGrouping("cmd-message-reader-spout","acq-config");

        builder.setSpout("hisdata-message-reader-spout",new HisDataSpout(),GroupingMethod.hisdataMessageReaderSpoutCount);
        builder.setSpout("rtdata-message-reader-spout",new RtDataSpout(),GroupingMethod.rtdataMessageReaderSpoutCount);
        ///////////////////////////////////////////////////////////////////////////////////////////
        builder.setBolt("data-process-bolt",new DataProcessBolt() ,GroupingMethod.dataCalculateBoltCount)
                .customGrouping("rtdata-message-reader-spout","calculate-data",new TagIdGrouping())
                .customGrouping("hisdata-message-reader-spout","calculate-data",new TagIdGrouping())
                .customGrouping("acq-config-bolt","data-process-config",new TagIdGrouping());


        builder.setBolt("his-data-save-bolt", new SaveDataBolt(),GroupingMethod.hisdataSaveBoltCount)
                .localOrShuffleGrouping("data-process-bolt","save-data");

        builder.setSpout("store-data-message-reader-spout",new StoreSpout(),GroupingMethod.storeDataMessageReaderSpoutCount);
        builder.setBolt("store-data-bolt",new StoreBolt(),GroupingMethod.storeDataBoltCount)
                .localOrShuffleGrouping("store-data-message-reader-spout","store-data");
    }
    //String dbHost,String dbName,String dbUser,String dbPassword,String redisHost
    public static void main(String[] args) throws InterruptedException {

        System.setProperty("log4j.configuration", "log4j.properties");
        //test();
        log.info("{}",log.isDebugEnabled()?"true":"false");
        log.debug("debug");
        log.info("info");
        log.warn("warn");
        log.error("error");
        Config conf = loadProperties();
        if(!loadAcqConfig(conf.get("db-host").toString(),conf.get("db-name").toString(),conf.get("db-user").toString(),conf.get("db-password").toString(),conf.get("redis-host").toString())){
            log.error("load acq config failed");
        }
        //定义拓扑
        TopologyBuilder builder = new TopologyBuilder();

        //builder.setSpout("message-reader-spout",new OriginDataSpout());
        ///////////////////////////////////////////////////////////////////////////////////////////

        topology2(builder);

        conf.setDebug(false);
        //运行拓扑
        //strom 流的产生会受到该参数抑制，即下游的bolt的tuple等待队列不能超过该参数，否则spout就暂停从消息源读取消息
//        storm里面topology.max.spout.pending属性解释：
//        1.同时活跃的batch数量，你必须设置同时处理的batch数量。你可以通过”topology.max.spout.pending” 来指定， 如果你不指定，默认是1。
//        2.topology.max.spout.pending 的意义在于 ，缓存spout 发送出去的tuple，当下流的bolt还有topology.max.spout.pending 个 tuple
//        没有消费完时，spout会停下来，等待下游bolt去消费，当tuple 的个数少于topology.max.spout.pending个数时，
//        spout 会继续从消息源读取消息。（这个属性只对可靠消息处理有用）
//        **************【在jstorm中，又将topology.max.spout.pending属性改成别的意义了】，如下：*************************************************
//        当topology.max.spout.pending 设置不为1时（包括topology.max.spout.pending设置为null），spout内部将额外启动一个线程单独执行ack或fail操作，
//        从而nextTuple在单独一个线程中执行，因此允许在nextTuple中执行block动作，而原生的storm，nextTuple/ack/fail 都在一个线程中执行，
//        当数据量不大时，nextTuple立即返回，而ack、fail同样也容易没有数据，进而导致CPU 大量空转，白白浪费CPU， 而在JStorm中，
//        nextTuple可以以block方式获取数据，比如从disruptor中或BlockingQueue中获取数据，当没有数据时，直接block住，节省了大量CPU。
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        boolean debugFlag = true;
        if(debugFlag)
        {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(VER, conf, builder.createTopology());
            Thread.sleep(60000000);
            cluster.killTopology(VER);
            cluster.shutdown();
        }
       else{
            try {
                StormSubmitter.submitTopology(VER,conf,builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        }
    }
}