/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * DateUtil.java
 * Created on  2018/3/13 13:18
 * 版本       修改时间          作者      修改内容
 *            2018/3/13      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.util;/*
 * DateUtil
 * Created on  202018/3/13 13:18  by liulin
 *  ${COPYRIGHT}
 */

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
    /*
     * 将时间转换为时间戳
     */
    public final static long WRONG_TIME = -1;
    public static long dateToStamp(String s,String pattern) /*throws ParseException*/ {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        Date date = null;
        try {
            date = simpleDateFormat.parse(s);
        } catch (ParseException e) {
            return WRONG_TIME;
        }

        return date.getTime();
    }

    public static long dateToStamp(String s) /*throws ParseException*/  {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = null;
        try {
            date = simpleDateFormat.parse(s);
        } catch (ParseException e) {
            return WRONG_TIME;
        }

        return date.getTime();
    }

    /*
     * 将时间戳转换为时间
     */
    public static String stampToDate(long s){
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(s);
        res = simpleDateFormat.format(date);
        return res;
    }

    public static String stampToDate(long s,String pattern){
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        Date date = new Date(s);
        res = simpleDateFormat.format(date);
        return res;
    }

}
