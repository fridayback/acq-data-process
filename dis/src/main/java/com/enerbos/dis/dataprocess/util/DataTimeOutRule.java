/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * DataTimeOutRule.java
 * Created on  2018/3/13 14:19
 * 版本       修改时间          作者      修改内容
 *            2018/3/13      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.util;/*
 * DataTimeOutRule
 * Created on  202018/3/13 14:19  by liulin
 *  ${COPYRIGHT}
 */

public class DataTimeOutRule extends AcqRule {

    private int timeout;
    private long lasttime;
    public DataTimeOutRule(String timeout,String org,String site,String tagid) throws Exception {
        super(timeout,org,site,tagid);
        //init(timeout);
    }

    public int getTimeout() {
        return timeout;
    }

    @Override
    protected boolean init(String s) throws Exception {
        switch (s){
            case "":
            case "null": {
                timeout = -1;
                break;
            }
            default: timeout = new Integer(s).intValue();
        }
        lasttime = System.currentTimeMillis();
        return true;
    }

    @Override
    public String toString() {
        return "DataTimeOutRule{" +
                "timeout=" + timeout +
                ", lasttime=" + lasttime +
                ", org='" + org + '\'' +
                ", site='" + site + '\'' +
                ", tagid='" + tagid + '\'' +
                '}';
    }
//    public boolean isTimeOut(long milsec){
//        return (lasttime+timeout <= milsec);
//    }
}
