/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * AcqConfig.java
 * Created on  2018/1/16 10:16
 * 版本       修改时间          作者      修改内容
 *            2018/1/16      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.util;/*
 * AcqConfig
 * Created on  202018/1/16 10:16  by liulin
 *  ${COPYRIGHT}
 */

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class AcqConfig {
    protected String org;
    protected String site;
    protected String chl;
    protected String type;
    public final static String CMD_ACACFG="conf_d";
    public final static String CMD_CTRL="ctrl_d";

    public final static String MSG_ORG="MSG_ORG";
    public final static String MSG_PRJ="MSG_PRJ";
    public final static String MSG_SYS="MSG_SYS";
    public final static String MSG_TYPE="MSG_TYPE";
    public final static String MSG_BODY="MSG_BODY";
    public final static String SUBSYSLIST="subsyslist";
    public final static String TERLIST="terlist";
    public final static String POINTS="points";
    public final static String TAGID="ptid";
    public final static String PTCLEAN="ptclean";
    public final static String PTSAVE="ptsave";
    public final static String PTTIMEOUT="pttimeout";
    public final static String FORMULA="formula";

    ArrayList points = new ArrayList<PointCfg>();

    @Override
    public String toString() {
        return "AcqConfig{" +
                "org='" + org + '\'' +
                ", site='" + site + '\'' +
                ", chl='" + chl + '\'' +
                ", type='" + type + '\'' +
                ", points=" + points +
                '}';
    }

    public AcqConfig(String jsonstr) throws IOException,NullPointerException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String,Object> map = mapper.readValue(jsonstr,Map.class);
        org = map.get(MSG_ORG).toString();
        site = map.get(MSG_PRJ).toString();
        chl = map.get(MSG_SYS).toString();
        type = map.get(MSG_TYPE).toString();
        if(!IsAcqConfigCmd()) return;
        Map body = (Map)(map.get(MSG_BODY));

        ArrayList subSystems = (ArrayList)((body).get(SUBSYSLIST));
        for (Object s:subSystems){
            ArrayList ters = (ArrayList)(((Map)s).get(TERLIST));
            for (Object t:ters){
                ArrayList pts = (ArrayList)(((Map)t).get(POINTS));
                for (Object p:pts){
                    Map ptMap = (Map)p;
                    String tagid = (String)ptMap.get(TAGID);
                    String clean = (String)ptMap.get(PTCLEAN);
                    String formula = (String)ptMap.get(FORMULA);
                    String save  = (String)ptMap.get(PTSAVE);
                    String timeout = (String)ptMap.get(PTTIMEOUT);
                    points.add( new PointCfg(tagid,org,site,clean, save,formula,timeout));
                }
            }
        }

    }

    public String getOrg() {
        return org;
    }

    public String getSite() {
        return site;
    }

    public ArrayList getPoints() {
        return points;
    }

    public boolean IsAcqConfigCmd(){return type.equals(CMD_ACACFG);}

    //public boolean IsCtrlCmd(){return type.equals(CMD_CTRL);}
}
