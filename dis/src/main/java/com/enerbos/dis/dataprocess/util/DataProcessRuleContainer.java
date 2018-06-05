/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * DataProcessRuleContainer.java
 * Created on  2018/3/27 14:34
 * 版本       修改时间          作者      修改内容
 *            2018/3/27      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.util;/*
 * DataProcessRuleContainer
 * Created on  202018/3/27 14:34  by liulin
 *  ${COPYRIGHT}
 */

import com.enerbos.dis.dataprocess.bolt.ConfigBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

enum RULE_TYPE{
    CAL_RULE,
    CLEAN_RULE,
    TIMEOUT_RULE,
    SAVE_RULE
}
public class DataProcessRuleContainer   {
    Map calculateRules = new HashMap<String,CalculateRule>();
    Map cleanRules = new HashMap<String,CleanRule>();
    Map timeoutRules = new HashMap<String,DataTimeOutRule>();
    Map saveRules = new HashMap<String,SaveRule>();
    Map dataProcessRule = new HashMap<String,DataProcessRule>();
    protected Map properties;
    private Jedis jedis;
    private Logger log = LoggerFactory.getLogger(AcqRuleContainer.class);

    //private Class ruleClass;
    public DataProcessRuleContainer(Map p) throws Exception{
        this.properties = p;
        init();
    }


    public boolean init() throws Exception {

        String redisHost = (String) properties.get("redis-host");
        String [] redisParam = redisHost.split(":");
        //if(redisParam.length != 2) return false;

        jedis = new Jedis(redisParam[0],new Integer(redisParam[1]).intValue());
        //jedis.auth("");

        return true;
    }

    public boolean unInit() {

        jedis.disconnect();
        return true;
    }

    public boolean synchronize(String key) {

        try {
            List<String> rule =  jedis.hmget((String)key, ConfigBolt.ORG,ConfigBolt.SITE,
                    CalculateRule.class.getSimpleName(),
                    CleanRule.class.getSimpleName(),
                    DataTimeOutRule.class.getSimpleName(),
                    SaveRule.class.getSimpleName());
            if(6 != rule.size() || null == rule.get(0) || null == rule.get(1) || null == rule.get(2)|| null == rule.get(3) || null == rule.get(4)|| null == rule.get(5)){
                return false;
            }
            java.lang.reflect.Constructor constructor = CalculateRule.class.getConstructor(String.class,String.class,String.class,String.class);
            CalculateRule calculateRule = (CalculateRule) constructor.newInstance(rule.get(2),rule.get(0),rule.get(1),(String)key);
            //calculateRules.put((String) key,calculateRule);
            constructor = CleanRule.class.getConstructor(String.class,String.class,String.class,String.class);
            CleanRule cleanRule = (CleanRule) constructor.newInstance(rule.get(3),rule.get(0),rule.get(1),(String)key);
            //cleanRules.put((String) key,cleanRule);
            constructor = DataTimeOutRule.class.getConstructor(String.class,String.class,String.class,String.class);
            DataTimeOutRule dataTimeOutRule = (DataTimeOutRule) constructor.newInstance(rule.get(4),rule.get(0),rule.get(1),(String)key);
            //timeoutRules.put((String) key,dataTimeOutRule);
            constructor = SaveRule.class.getConstructor(String.class,String.class,String.class,String.class);
            SaveRule saveRule = (SaveRule) constructor.newInstance(rule.get(5),rule.get(0),rule.get(1),(String)key);
            //saveRules.put((String) key,saveRule);
            DataProcessRule r = new DataProcessRule(calculateRule,cleanRule,dataTimeOutRule,saveRule);
            dataProcessRule.put(key,r);
            return true;
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }catch (JedisException e){
            e.printStackTrace();
        }

        return false;
    }

    public boolean save(String key,DataProcessRule r) {
        dataProcessRule.put(key,r);
        return true;
    }


    public DataProcessRule getRule(String key) {
        DataProcessRule r = (DataProcessRule)dataProcessRule.get(key);
        if(null == r)
        {
            if(!synchronize(key)) return null;
            r = (DataProcessRule)dataProcessRule.get(key);
        }
        return r;
    }

}
