/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * RuleContainer.java
 * Created on  2018/1/5 17:28
 * 版本       修改时间          作者      修改内容
 *            2018/1/5      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.util;/*
 * RuleContainer
 * Created on  202018/1/5 17:28  by liulin
 *  ${COPYRIGHT}
 */

import java.util.Map;

public abstract class RuleContainer {
    //HashMap rules;
    //protected ConcurrentHashMap<String,BaseRule> rules;
    protected Map<String,BaseRule> rules;
    protected Map properties;
    public RuleContainer(Map p){
        properties = p;
    }

    public BaseRule getRule(String key){
        return rules.get(key);
    }
    public abstract boolean init() throws Exception;
    public abstract boolean unInit();
}
