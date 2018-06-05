/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * DisMessage.java
 * Created on  2018/2/28 17:17
 * 版本       修改时间          作者      修改内容
 *            2018/2/28      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.util;/*
 * DisMessage
 * Created on  202018/2/28 17:17  by liulin
 *  ${COPYRIGHT}
 */

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DisMessage {
    protected String org;
    protected String site;
    protected String type;
    protected Object data;
    protected String chl;
    protected String collector;

    public final static String MS_HISDATA="dh_u";
    public final static String MS_RTDATA="dr_u";
    public final static String MS_ACQCFGCMD="conf_d";
    public final static String MS_CTRLCMD="ctrl_d";

    public final static String MSG_ORG="MSG_ORG";
    public final static String MSG_PRJ="MSG_PRJ";
    public final static String MSG_SYS="MSG_SYS";
    public final static String MSG_TYPE="MSG_TYPE";
    public final static String MSG_DEV="MSG_DEV";
    public final static String MSG_BODY="MSG_BODY";

    public final static String PT_NAME="ptname";//"ptid";
    public final static String PT_TIME="pttime";
    public final static String PT_VALUE="ptvalue";

    public final static String SUBSYSLIST="subsyslist";
    public final static String TERLIST="terlist";
    public final static String POINTS="points";
    public final static String TAGID="ptid";
    public final static String PTCLEAN="ptclean";
    public final static String PTSAVE="ptsave";
    public final static String PTTIMEOUT="pttimeout";
    public final static String FORMULA="formula";

    public String getOrg() {
        return org;
    }

    public String getSite() {
        return site;
    }

    public String getType() {
        return type;
    }

    public Object getData() {
        return data;
    }

    public String getChl() {
        return chl;
    }

    public String getCollector() {
        return collector;
    }

    public DisMessage(String jsonstr) throws NullPointerException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String,Object> map = mapper.readValue(jsonstr,Map.class);
        org = map.get(MSG_ORG).toString();
        site = map.get(MSG_PRJ).toString();
        chl = map.get(MSG_SYS).toString();
        collector = map.get(MSG_DEV).toString();
        type = map.get(MSG_TYPE).toString();
        //
        switch (type){
            case MS_HISDATA:
            case MS_RTDATA: {
                ArrayList<Object> pointDatas = (ArrayList<Object>)(map.get(MSG_BODY));
                ArrayList dataList = new ArrayList<PointValue>();
                for (Object d:pointDatas){
                    Map<String,Object> di =  (Map)d;

                    String ptname = di.get(PT_NAME).toString();
                    String pttime = di.get(PT_TIME).toString();
                    double ptvalue = new Double(di.get(PT_VALUE).toString()).doubleValue();
                    dataList.add( new PointValue(ptname,pttime,ptvalue));
                }
                data = dataList;
                break;
            }
            case MS_ACQCFGCMD:{
                Map body = (Map)map.get(MSG_BODY);
                ArrayList subSystems = (ArrayList)((body).get(SUBSYSLIST));
                List allRules = new ArrayList<Map>();
                for (Object s:subSystems){
                    ArrayList ters = (ArrayList)(((Map)s).get(TERLIST));
                    for (Object t:ters){
                        ArrayList pts = (ArrayList)(((Map)t).get(POINTS));
                        for (Object p:pts){
                            Map ptMap = (Map)p;
                            String tagid = ptMap.get(TAGID).toString();
                            String cleanRule = ptMap.get(PTCLEAN).toString();
                            String calculateRule = ptMap.get(FORMULA).toString();
                            String timeoutRule =  ptMap.get(PTTIMEOUT).toString();
                            String saveRule = ptMap.get(PTSAVE).toString();

                            PointCfg ptCfg = new PointCfg(tagid,org,site,cleanRule,saveRule,calculateRule,timeoutRule);

                            allRules.add(ptCfg);
                        }
                    }
                }
                data = allRules;
                break;
            }
            default:
        }
    }


    @Override
    public String toString() {
        return "DisMessage{" +
                "org='" + org + '\'' +
                ", site='" + site + '\'' +
                ", type='" + type + '\'' +
                ", data=" + data +
                ", chl='" + chl + '\'' +
                ", collector='" + collector + '\'' +
                '}';
    }
}
