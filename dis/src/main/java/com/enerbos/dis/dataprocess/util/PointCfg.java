/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * PointCfg.java
 * Created on  2018/1/16 15:04
 * 版本       修改时间          作者      修改内容
 *            2018/1/16      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.util;/*
 * PointCfg
 * Created on  202018/1/16 15:04  by liulin
 *  ${COPYRIGHT}
 */

public class PointCfg {
    String tagId;
    String ptClean;
    String ptSave;
    String formula;
    String ptTimeout;
    String org;
    String site;
    public  PointCfg(String tagId,String org,String site,String ptClean,String ptSave,String formula,String ptTimeout){
        this.tagId = tagId;
        this.ptClean = ptClean;
        this.formula = formula;
        this.ptSave = ptSave;
        this.ptTimeout = ptTimeout;
        this.org = org;
        this.site = site;
        switch (this.ptClean){
            case "null": {
                this.ptClean = "";
                break;
            }
            default:
        }
        switch (this.formula){
            case "null": {
                this.formula = "1";
                break;
            }
            default:
        }
        switch (this.ptSave){
            case "null": {
                this.ptSave = "0";
                break;
            }
            default:
        }
        switch (this.ptTimeout){
            case "null": {
                this.ptTimeout = "-1";
                break;
            }
            default:
        }
    }

    @Override
    public String toString() {
        return "PointCfg{" +
                "tagId='" + tagId + '\'' +
                ", ptClean='" + ptClean + '\'' +
                ", ptSave=" + ptSave +
                ", formula='" + formula + '\'' +
                ", ptTimeout=" + ptTimeout +
                ", org='" + org + '\'' +
                ", site='" + site + '\'' +
                '}';
    }

    public String getTagId() {
        return tagId;
    }

    public String getPtClean() {
        return ptClean;
    }

    public String getPtSave() {
        return ptSave;
    }

    public String getFormula() {
        return formula;
    }

    public String getPtTimeout() {
        return ptTimeout;
    }

    public String getOrg() {
        return org;
    }

    public String getSite() {
        return site;
    }
}
