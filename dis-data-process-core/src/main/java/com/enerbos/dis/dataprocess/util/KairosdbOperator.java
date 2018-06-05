/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

/*
 * KairosdbOperator.java
 * Created on  2018/3/19 14:14
 * 版本       修改时间          作者      修改内容
 *            2018/3/19      liulin    初始版本
 */
package com.enerbos.dis.dataprocess.util;/*
 * KairosdbOperator
 * Created on  202018/3/19 14:14  by liulin
 *  ${COPYRIGHT}
 */

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

public class KairosdbOperator {

    private static CloseableHttpClient httpClient = HttpClients.custom().disableAutomaticRetries().build();
    private static RequestConfig  requestConfig = RequestConfig.custom().setSocketTimeout(30000).setConnectTimeout(1000).build();

    private Logger log = LoggerFactory.getLogger(KairosdbOperator.class);

    private boolean post(String strUrl,String params){

        HttpPost post = new HttpPost(strUrl);
        post.setConfig(requestConfig);
        post.addHeader("content-type","application/json");
        post.setHeader("Accept", "application/json");
        try {
            post.setEntity(new StringEntity(params));
            HttpResponse httpResponse = httpClient.execute(post);
            int  statusCode = httpResponse.getStatusLine().getStatusCode();
            if(statusCode!=204) {
                log.error("", EntityUtils.toString(httpResponse.getEntity()));
                return false;
            }
        } catch (UnsupportedEncodingException e) {
            log.error("",e);
            return false;
        } catch (ClientProtocolException e) {
            log.error("",e);
            return false;
        } catch (IOException e) {
            log.error("",e);
            return false;
        }


        return true;
    }

    private String kairosdbHost;//http://[host]:[port]/api/v1/datapoints/delete
    private static final int timeout = 10000;

    public KairosdbOperator(String kairosdbHost) {
        this.kairosdbHost = kairosdbHost;
    }

    private String getSaveURL(){
        return ("http://"+kairosdbHost+"/api/v1/datapoints");
    }
    private String getDeleteURL(){
        return ("http://"+kairosdbHost+"/api/v1/datapoints/delete");
    }
    public boolean saveDatas(List datas){

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String jsonStr = objectMapper.writeValueAsString(datas);
            String url = getSaveURL();
            return post(url,jsonStr);
        } catch (JsonProcessingException e) {
           log.error("{}",e);
        }
        return false;
    }
    public boolean deleteData(String tagid,long beginTime,long endTime){
        return false;
    }
    public boolean updateData(PointValue pv){
        return false;
    }
}
