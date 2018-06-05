/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * GroupingMethod.java
 * Created on  2018/3/23 17:18
 * 版本       修改时间          作者      修改内容
 *            2018/3/23      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.util;/*
 * GroupingMethod
 * Created on  202018/3/23 17:18  by liulin
 *  ${COPYRIGHT}
 */

public class GroupingMethod {
    public static int cmdMessageReaderSpoutCount = 1;
    public static int acqConfigBoltCount = 1;
    public static int hisdataMessageReaderSpoutCount = 1;
    public static int rtdataMessageReaderSpoutCount = 1;
    public static int dataCalculateBoltCount = 1;
    public static int dataCleanBoltCount = 1;
    public static int rtdataSaveBoltCount = 1;
    public static int hisdataSaveBoltCount = 1;
    public static int storeDataMessageReaderSpoutCount = 1;
    public static int storeDataBoltCount = 1;
    public static int dataProcessBoltCount = 1;
    public static int getGroupIdByKey(String key,int count){
        return key.hashCode()%count;
    }
}
