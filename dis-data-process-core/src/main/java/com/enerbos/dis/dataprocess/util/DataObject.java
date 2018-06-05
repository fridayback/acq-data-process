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
 * DataObject.java
 * Created on  2018/1/4 14:32
 * 版本       修改时间          作者      修改内容
 *            2018/1/4      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.util;/*
 * DataObject
 * Created on  202018/1/4 14:32  by liulin
 *  ${COPYRIGHT}
 */


import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class DataObject {

    protected String org;
    protected String site;
    protected String chl;
    protected String type;
    public final static String HISDATA="dh_u";
    public final static String RTDATA="dr_u";

    public final static String MSG_ORG="MSG_ORG";
    public final static String MSG_PRJ="MSG_PRJ";
    public final static String MSG_SYS="MSG_SYS";
    public final static String MSG_TYPE="MSG_TYPE";
    public final static String MSG_BODY="MSG_BODY";
    public final static String PT_NAME="ptid";//"ptname";
    public final static String PT_TIME="pttime";
    public final static String PT_VALUE="ptvalue";

    ArrayList datas = new ArrayList<PointValue>();

    @Override
    public String toString() {
        return "DataObject{" +
                "org:\"" + org + '"' +
                ", site:\"" + site + '"' +
                ", chl:\"" + chl + '"' +
                ", type:\"" + type + '"' +
                ", datas:" + datas +
                '}';
    }

    public DataObject(String jsonstr) throws IOException,NullPointerException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String,Object> map = mapper.readValue(jsonstr,Map.class);
        org = map.get(MSG_ORG).toString();
        site = map.get(MSG_PRJ).toString();
        chl = map.get(MSG_SYS).toString();
        type = map.get(MSG_TYPE).toString();
        ArrayList<Object> pointDatas = (ArrayList<Object>)(map.get(MSG_BODY));
        for (Object d:pointDatas){
            Map<String,Object> di =  (Map)d;

            String ptname = di.get(PT_NAME).toString();
            String pttime = di.get(PT_TIME).toString();
            double ptvalue = new Double(di.get(PT_VALUE).toString()).doubleValue();
            datas.add( new PointValue(ptname,pttime,ptvalue));
        }
    }

    public String getOrg() {
        return org;
    }

    public void setOrg(String org) {
        this.org = org;
    }

    public String getSite() {
        return site;
    }

    public void setSite(String site) {
        this.site = site;
    }

    public String getChl() {
        return chl;
    }

    public void setChl(String chl) {
        this.chl = chl;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public ArrayList getDatas() {
        return datas;
    }

    public void setDatas(ArrayList datas) {
        this.datas = datas;
    }
}
