/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * PointValue.java
 * Created on  2018/1/4 14:50
 * 版本       修改时间          作者      修改内容
 *            2018/1/4      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.util;/*
 * PointValue
 * Created on  202018/1/4 14:50  by liulin
 *  ${COPYRIGHT}
 */

public class PointValue {
    public static final int STATUS_OK = 0;
    public static final int STATUS_OUTOFBOUDN = 3;
    public static final int STATUS_ACQ_ERR = 1;
    String tagid;
    String time;
    String org;
    String site;
    double ptvalue;
    int status = STATUS_OK;


    public PointValue(String tagid,String time,double val){
        this.tagid = tagid;
        this.time = time;
        this.ptvalue = val;
    }

    @Override
    public String toString() {
        return "PointValue{" +
                "tagid='" + tagid + '\'' +
                ", time='" + time + '\'' +
                ", ptvalue=" + ptvalue +
                ", status=" + status +
                '}';
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getTagid() {
        return tagid;
    }

    public void setTagId(String tagid) {
        this.setTagId(tagid);;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public double getPtvalue() {
        return ptvalue;
    }

    public void setPtvalue(double ptvalue) {
        this.ptvalue = ptvalue;
    }

    public String getOrg() {
        return org;
    }

    public String getSite() {
        return site;
    }

    public void setOrg(String org) {
        this.org = org;
    }

    public void setSite(String site) {
        this.site = site;
    }
}
