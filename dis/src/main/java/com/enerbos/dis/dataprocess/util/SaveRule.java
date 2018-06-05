/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * SaveRule.java
 * Created on  2018/3/13 12:39
 * 版本       修改时间          作者      修改内容
 *            2018/3/13      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.util;/*
 * SaveRule
 * Created on  202018/3/13 12:39  by liulin
 *  ${COPYRIGHT}
 */

//import static com.enerbos.dis.dataprocess.util.SaveRule.SaveType.NO_SAVE;
enum SaveType{
    SAVE_CHANGE,//变化存
    NO_SAVE,//不存储
    SAVE_5MIN ,//5分钟定时存
    SAVE_ANYWAY //有数据就存
};
public class SaveRule extends AcqRule {

    private PointValue lastValue;
    private PointValue thisValue;
    private SaveType saveConfig;
    private long last5mTime = 0;
    private long this5mTime = 0;
    @Override
    protected boolean init(String s) throws Exception {
        switch (s){
            case "":
            case "null": {
                s = "0";
                break;
            }
            default:
        }

        int tmp = new Integer(s).intValue();
        switch (tmp){
            case -1:{
                this.saveConfig = SaveType.SAVE_CHANGE;
                break;
            }
            case 0:{
                this.saveConfig = SaveType.NO_SAVE;
                break;
            }
            case 1:{
                this.saveConfig = SaveType.SAVE_5MIN;
                break;
            }
            case -2:{
                this.saveConfig = SaveType.SAVE_ANYWAY;
                break;
            }
            default:{

            }
        }

        return true;
    }

    public SaveRule(String saveConfig, String org, String site, String tagId) throws Exception {
        super(saveConfig,org,site,tagId);
    }

    public void rollback(){
        switch (saveConfig){
            case NO_SAVE:
                return;
            case SAVE_CHANGE:
                thisValue = lastValue;
                break;
            case SAVE_5MIN:
                this5mTime = last5mTime;
                break;
        }
    }
    public PointValue getSavaData(PointValue p){
        switch (saveConfig){
            case NO_SAVE:
                return null;

            case SAVE_CHANGE:
                lastValue = thisValue;
                if(null == lastValue){
                    thisValue = p;
                    return p;
                }
                else if(false == p.getTagid().equals(lastValue.getTagid()) || p.getPtvalue() == lastValue.getPtvalue()) return null;
                else {
                    thisValue = p;
                    return p;
                }
            case SAVE_5MIN:{
                last5mTime = this5mTime;
                long new5mTime = DateUtil.dateToStamp(p.getTime());
                if(DateUtil.WRONG_TIME == new5mTime) return null;
                new5mTime = new5mTime - new5mTime%300000;
                if(last5mTime != new5mTime){
                    this5mTime = new5mTime;
                    p.time = DateUtil.stampToDate(new5mTime);
                    return p;
                }
                else return null;
            }
            case SAVE_ANYWAY:{
                thisValue = p;
                return p;
            }
        }

        return null;
    }

    @Override
    public String toString() {
        return "SaveRule{" +
                "lastValue=" + lastValue +
                ", saveConfig=" + saveConfig +
                ", last5mTime=" + last5mTime +
                ", org='" + org + '\'' +
                ", site='" + site + '\'' +
                ", tagid='" + tagid + '\'' +
                '}';
    }
}
