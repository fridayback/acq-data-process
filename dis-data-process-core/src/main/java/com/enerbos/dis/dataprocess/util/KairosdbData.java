/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * KairosdbData.java
 * Created on  2018/3/19 15:44
 * 版本       修改时间          作者      修改内容
 *            2018/3/19      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.util;/*
 * KairosdbData
 * Created on  202018/3/19 15:44  by liulin
 *  ${COPYRIGHT}
 */

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class KairosdbData {
    public String name;
    public long timestamp;
    public String value;
    //public int status;
    public Map tags;

    public  static String getYearTag(long sec){
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(sec), TimeZone.getTimeZone("GMT+:08:00").toZoneId());
        return String.format("%02d",localDateTime.getYear()%100);
    }

    public  static String getMonthTag(long sec){
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(sec), TimeZone.getTimeZone("GMT+:08:00").toZoneId());
        return String.format("%02d",localDateTime.getMonth());
    }
    public  static String getDayTag(long sec){
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(sec), TimeZone.getTimeZone("GMT+:08:00").toZoneId());
        return String.format("%02d",localDateTime.getDayOfMonth());
    }
    public  static String getHourTag(long sec){
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(sec), TimeZone.getTimeZone("GMT+:08:00").toZoneId());
        return String.format("%02d",localDateTime.getHour());
    }
    public  static String getMinTag(long sec){
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(sec), TimeZone.getTimeZone("GMT+:08:00").toZoneId());
        return String.format("%02d",localDateTime.getMinute());
    }
    public  static String getSecTag(long sec){
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(sec), TimeZone.getTimeZone("GMT+:08:00").toZoneId());
        return String.format("%02d",localDateTime.getSecond());
    }
    public  static String getWeekTag(long sec){
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(sec), TimeZone.getTimeZone("GMT+:08:00").toZoneId());
        return String.format("%02d",localDateTime.getDayOfWeek());
    }

    public void genTags(String org,String site,int status){
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(timestamp/1000), TimeZone.getDefault().toZoneId());
        tags = new HashMap();
        tags.put("year",String.format("%02d",localDateTime.getYear()%100));
        tags.put("mon",String.format("%02d",localDateTime.getMonth().getValue()));
        tags.put("day",String.format("%02d",localDateTime.getDayOfMonth()));
        tags.put("hour",String.format("%02d",localDateTime.getHour()));
        tags.put("min",String.format("%02d",localDateTime.getMinute()));
        tags.put("sec",String.format("%02d",localDateTime.getSecond()));
        tags.put("week",String.format("%02d",localDateTime.getDayOfWeek().getValue()));
        tags.put("org",org);
        tags.put("site",site);
        tags.put("status",new Integer(status).toString());
    }

}
