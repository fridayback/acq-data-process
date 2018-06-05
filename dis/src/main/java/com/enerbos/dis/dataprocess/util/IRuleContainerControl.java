/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * IRule.java
 * Created on  2018/1/5 17:18
 * 版本       修改时间          作者      修改内容
 *            2018/1/5      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.util;/*
 * IRule
 * Created on  202018/1/5 17:18  by liulin
 */

public interface IRuleContainerControl {
    boolean synchronize(Object key);
    boolean save(Object key,BaseRule rule);
}
