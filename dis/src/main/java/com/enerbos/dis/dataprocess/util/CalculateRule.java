/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * CalculateRule.java
 * Created on  2018/3/12 17:45
 * 版本       修改时间          作者      修改内容
 *            2018/3/12      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.util;/*
 * CalculateRule
 * Created on  202018/3/12 17:45  by liulin
 *  ${COPYRIGHT}
 */

public class CalculateRule extends AcqRule {
    private double formula;

    public CalculateRule(String s, String org, String site, String tagid) throws Exception {
        super(s,org,site,tagid);
    }

    @Override
    public String toString() {
        return "CalculateRule{" +
                "formula=" + formula +
                ", org='" + org + '\'' +
                ", site='" + site + '\'' +
                ", tagid='" + tagid + '\'' +
                '}';
    }

    @Override
    protected boolean init(String s) throws Exception {
        switch (s){
            case "":
            case "null":{
                formula = 1.;
                break;
            }
            default: formula = new Double(s).doubleValue();
        }
        return true;
    }

    public PointValue getCalaulteResult(PointValue value){
        if(-999. != value.getPtvalue() && -888. != value.getPtvalue())
            value.setPtvalue(value.getPtvalue()*formula);
        return value;
    }
    public String getSite() {
        return site;
    }

    public String getTagid() {
        return tagid;
    }

    public String getOrg() {
        return org;
    }

    public void setSite(String site) {
        this.site = site;
    }

    public void setTagid(String tagid) {
        this.tagid = tagid;
    }

    public void setOrg(String org) {
        this.org = org;
    }
}
