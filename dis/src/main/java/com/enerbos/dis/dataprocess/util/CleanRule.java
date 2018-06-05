/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * CleanRule.java
 * Created on  2018/1/5 17:10
 * 版本       修改时间          作者      修改内容
 *            2018/1/5      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.util;/*
 * CleanRule
 * Created on  202018/1/5 17:10  by liulin
 *  ${COPYRIGHT}
 */

import java.util.ArrayList;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CleanRule extends AcqRule{

    private ArrayList<IIsIn> childRules;
    static final String regex = "([{]\\s*(?:[-+]?\\d+(?:\\.\\d+)?)(\\s*,\\s*(?:[-+]?\\d+(?:\\.\\d+)?))*\\s*[}])|([\\[\\(]\\s*(?:(?:(?:[-+]?\\d+(?:\\.\\d+)?))|(?:-∝))\\s*,\\s*(?:(?:(?:[-+]?\\d+(?:\\.\\d+)?))|(?:\\+∝))\\s*[\\]\\)])";
    class Discrete implements IIsIn{
        static final String regex = "([-+]?\\d+(?:\\.\\d+)?)";
        private HashSet values = new HashSet<Double>();

        //离散值合并
        void merge(Discrete d){
            if (this == d) return;
            values.addAll(d.values);
        }
        //是否在集合中

        public  Discrete(String s){
            Pattern r = Pattern.compile(regex);
            Matcher matcher = r.matcher(s);
            while(matcher.find()){
                //获取 字符串
                String group = matcher.group();
                values.add(new Double(group));
            }
        }

        @Override
        public String toString() {
            return "Discrete{" +
                    "values=" + values +
                    '}';
        }

        @Override
        public boolean isIn(Object o) {
            return values.contains(o);
        }

        @Override
        public boolean isIn(double d) {
            return values.contains(new Double(d));
        }
    }
    class Interval implements IIsIn{
        static final String regex = "((?:[-+]?\\d+(?:\\.\\d+)?))|((?:\\+|-)∝)|\\[|\\]|\\(|\\)";
        private double low;
        private boolean bLow = false;
        private double up;
        private boolean bUp = false;


        public Interval(String s) throws Exception{
            Pattern r = Pattern.compile(regex);
            Matcher matcher = r.matcher(s);
            String limit[] = {"","","",""};
            int count = 0;
            while(matcher.find()){
                //获取 字符串
                limit[count++] = matcher.group();
            }
            if(("").equals(limit[0]) || ("").equals(limit[1])|| ("").equals(limit[2])|| ("").equals(limit[3])) {
                throw new Exception("bad clean rule string:"+s);
            }
            //是否可等于左极限值
            if (("[").equals(limit[0])){
                bLow = true;
            }
            //解析左极限值
            if (limit[1].indexOf('∝') >= 0){
                low = Double.NEGATIVE_INFINITY;
                bLow = false;
            }else{
                low = new Double(limit[1]).doubleValue();
            }

            //是否可等右极限值
            if (("[").equals(limit[3])){
                bUp = true;
            }
            //解析右极限值
            if (limit[2].indexOf('∝') >= 0){
                up = Double.POSITIVE_INFINITY;
                bUp = false;
            }else{
                up = new Double(limit[2]).doubleValue();
            }
        }

        @Override
        public boolean isIn(double d){
            if(d>low && d < up) return true;
            if(d == low && bLow) return true;
            if (d == up && bUp) return true;
            return false;
        }

        @Override
        public String toString() {
            return "Interval{" +
                    "low=" + low +
                    ", bLow=" + bLow +
                    ", up=" + up +
                    ", bUp=" + bUp +
                    '}';
        }

        @Override
        public boolean isIn(Object o) {
            if (o instanceof Number) return isIn(((Number) o).doubleValue());
            return false;
        }
    }


    @Override
    protected boolean init(String s) throws Exception {
        childRules = new ArrayList<IIsIn>();
        if (s.equals("") || s.equals("null"))
        {
            s = "(-∝,+∝)";
        }
        if (!check(s)){
            throw new Exception("bad clean rule string:"+s);
        }
        Pattern r = Pattern.compile(regex);
        Matcher matcher = r.matcher(s);
        ArrayList list = new ArrayList<String>();
        while(matcher.find()){
            //获取 字符串
            String group = matcher.group();
            if(group.indexOf('{') >=0) {
                Discrete discrete = new Discrete(group);
                childRules.add(discrete);
            }else {
                Interval interval = new Interval(group);
                childRules.add(interval);
            }

        }
        return true;
    }

    public CleanRule(String s, String org, String site, String tagid) throws Exception {
        super(s,org,site,tagid);
    }

    public static boolean check(String s){
        if(("").equals(s)) return true;
        Pattern r = Pattern.compile(regex);
        Matcher matcher = r.matcher(s);
        String replaceString = matcher.replaceAll("");
        return ("").equals(replaceString.replaceAll("\\s*[,]*\\s*",""));
    }

    protected boolean isIn(double d){

        for (IIsIn childRule: childRules){
            if (childRule.isIn(d)) return true;
        }
        return false;
    }
    public PointValue clean(PointValue pv){
        if (pv.getStatus() == PointValue.STATUS_OK && !isIn(pv.getPtvalue())){
            pv.setStatus(PointValue.STATUS_OUTOFBOUDN);
        }
        return pv;
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

    @Override
    public String toString() {
        return "CleanRule{" +
                "childRules=" + childRules +
                ", org='" + org + '\'' +
                ", site='" + site + '\'' +
                ", tagid='" + tagid + '\'' +
                '}';
    }
}
