/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * TagTransDataObject.java
 * Created on  2018/3/14 10:46
 * 版本       修改时间          作者      修改内容
 *            2018/3/14      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.util;/*
 * TagTransDataObject
 * Created on  202018/3/14 10:46  by liulin
 *  ${COPYRIGHT}
 */

public class TagTransDataObject {
    public String tagid;
    public Object data;

    public TagTransDataObject(String tagid, Object data) {
        this.tagid = tagid;
        this.data = data;
    }

    public String getTagid() {
        return tagid;
    }

    public Object getData() {
        return data;
    }
}
