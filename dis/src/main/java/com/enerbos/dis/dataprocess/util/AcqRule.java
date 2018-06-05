/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * AcqRule.java
 * Created on  2018/3/13 14:37
 * 版本       修改时间          作者      修改内容
 *            2018/3/13      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.util;/*
 * AcqRule
 * Created on  202018/3/13 14:37  by liulin
 *  ${COPYRIGHT}
 */

public abstract class AcqRule extends BaseRule {
    protected String org;
    protected String site;
    protected String tagid;

    protected abstract boolean init(String s) throws Exception;

    public AcqRule(String rule,String org, String site, String tagid) throws Exception {
        this.org = org;
        this.site = site;
        this.tagid = tagid;
        init(rule);
    }

    public String getOrg() {
        return org;
    }

    public String getSite() {
        return site;
    }

    public String getTagid() {
        return tagid;
    }
}
