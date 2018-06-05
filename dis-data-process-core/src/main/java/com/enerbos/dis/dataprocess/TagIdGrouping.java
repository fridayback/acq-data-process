/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * TagIdGrouping.java
 * Created on  2018/3/23 16:20
 * 版本       修改时间          作者      修改内容
 *            2018/3/23      liulin    初始版本
 */
package com.enerbos.dis.dataprocess;/*
 * TagIdGrouping
 * Created on  202018/3/23 16:20  by liulin
 *  ${COPYRIGHT}
 */

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.ArrayList;
import java.util.List;

public class TagIdGrouping implements CustomStreamGrouping {
    int taskNumber = 0;
    List<Integer> bolts;
    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> list) {
        taskNumber = list.size();
        bolts = new ArrayList<>();
        bolts.addAll(list);
    }

    @Override
    public List<Integer> chooseTasks(int i, List<Object> list) {
        List<Integer> boltIds = new ArrayList<>();
        if (list.size() > 0) {
            //System.out.println("["+list.get(0).getClass().getSimpleName()+"]");
           // System.out.println("!"+list.toString()+"!");
//            if(list.get(0).getClass().getSimpleName() == "String"){
//                System.out.println(list.get(0));
//            }
            try{
                Integer id = (Integer) list.get(0);

                boltIds.add(bolts.get(id.intValue()));
            }catch (Exception e){
                System.out.println(list.get(0));
            }

    }

        return boltIds;
    }
}
