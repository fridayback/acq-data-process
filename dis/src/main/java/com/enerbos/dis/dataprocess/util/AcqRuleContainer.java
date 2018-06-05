/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * CleanRuleContainer.java
 * Created on  2018/1/12 15:54
 * 版本       修改时间          作者      修改内容
 *            2018/1/12      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.util;/*
 * CleanRuleContainer
 * Created on  202018/1/12 15:54  by liulin
 *  ${COPYRIGHT}
 */

import com.enerbos.dis.dataprocess.bolt.ConfigBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AcqRuleContainer extends RuleContainer implements IRuleContainerControl {
    private Jedis jedis;
    private Logger log = LoggerFactory.getLogger(AcqRuleContainer.class);

    private Class ruleClass;
    public AcqRuleContainer(Map p,Class ruleClass) throws Exception{
        super(p);
        rules = new ConcurrentHashMap<String,BaseRule>();
        this.ruleClass = ruleClass;
        init();
    }

    @Override
    public boolean init() throws Exception {

        String redisHost = (String) properties.get("redis-host");
        String [] redisParam = redisHost.split(":");
        //if(redisParam.length != 2) return false;

        jedis = new Jedis(redisParam[0],new Integer(redisParam[1]).intValue());
        //jedis.auth("");

        return true;
    }

    @Override
    public boolean unInit() {

        jedis.disconnect();
        return true;
    }

    @Override
    public boolean synchronize(Object key) {
        List<String>rule =  jedis.hmget((String)key, ConfigBolt.ORG,ConfigBolt.SITE,ruleClass.getSimpleName());
        if(3 != rule.size() || null == rule.get(0) || null == rule.get(1) || null == rule.get(2)){
            return false;
        }
        try {
            java.lang.reflect.Constructor constructor = ruleClass.getConstructor(String.class,String.class,String.class,String.class);
            AcqRule r = (AcqRule) constructor.newInstance(rule.get(2),rule.get(0),rule.get(1),(String)key);
            rules.put((String) key,r);
            return true;
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }

        return false;
    }

    @Override
    public boolean save(Object key,BaseRule rule) {
        rules.put((String) key,rule);
        return true;
    }

    @Override
    public BaseRule getRule(String key) {
        if(null == super.getRule(key))
        {
            if(!synchronize(key)) return null;
        }
        return super.getRule(key);
    }

    //    @Override
//    public boolean synchronize(Object key) {
//        String site = (String)key;
//        String url = "jdbc:mysql://"+dbHost+"/"+dbName;
//        HashMap<String,List<String>> newRules = new HashMap();
//        try {
//            Connection conn = DriverManager.getConnection(url, dbUser, dbPassword);
//
//            if(conn.isClosed()){
//                log.error("config synchronize failed: connect to db:{} is closed",this.dbHost);
//                return false;
//            }
//
//            String sql = sqlQuery.replace("##SITE##",site);
//            Statement stmt = conn.createStatement();
//            ResultSet rs = stmt.executeQuery(sql);
//
//            while(rs.next()){
//                String cleanRule = "";
//                cleanRule = rs.getString("pt_clean");
//                switch (cleanRule){
//                    case "null": {
//                        cleanRule = "";
//                        break;
//                    }
//                    default:;
//                }
//                newRules.put(rs.getString("tagid"), Arrays.asList(cleanRule,rs.getString("org"),rs.getString("site")));
//            }
//            rs.close();
//            stmt.close();
//            conn.close();
//
//        }catch(SQLException e) {
//            log.error("config synchronize failed:",e);
//            return false;
//        } catch(Exception e) {
//            log.error("config synchronize failed:",e);
//            return false;
//        }
//
//        //////////////////////////////////////////////////////////
//        for (Iterator<Map.Entry<String,BaseRule>> it = rules.entrySet().iterator();it.hasNext();){
//            Map.Entry<String,BaseRule> item = it.next();
//            if (((CleanRule)item.getValue()).getSite() == key && null == newRules.get(item.getKey()) ){
//                it.remove();
//            }
//        }
//        for (Map.Entry<String,List<String>> entry:newRules.entrySet()){
//            try {
//                BaseRule rule = new CleanRule(entry.getValue().get(0), entry.getValue().get(1),entry.getValue().get(2) ,entry.getKey());
//
//                rules.put(entry.getKey(),rule);
//            } catch (Exception e) {
//                log.error("update clean rule failed:{} {}:",entry.getKey(),entry.getValue(),e);
//            }
//        }
//
//        return true;
//    }



}
