/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * DataProcessRule.java
 * Created on  2018/4/2 13:26
 * 版本       修改时间          作者      修改内容
 *            2018/4/2      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.util;/*
 * DataProcessRule
 * Created on  202018/4/2 13:26  by liulin
 *  ${COPYRIGHT}
 */

public class DataProcessRule {
    CalculateRule calculateRule;
    CleanRule cleanRule;
    DataTimeOutRule dataTimeOutRule;
    SaveRule saveRule;

    public DataProcessRule(CalculateRule calculateRule, CleanRule cleanRule, DataTimeOutRule dataTimeOutRule, SaveRule saveRule) {
        this.calculateRule = calculateRule;
        this.cleanRule = cleanRule;
        this.dataTimeOutRule = dataTimeOutRule;
        this.saveRule = saveRule;
    }

    public CalculateRule getCalculateRule() {
        return calculateRule;
    }

    public CleanRule getCleanRule() {
        return cleanRule;
    }

    public DataTimeOutRule getDataTimeOutRule() {
        return dataTimeOutRule;
    }

    public SaveRule getSaveRule() {
        return saveRule;
    }
}
