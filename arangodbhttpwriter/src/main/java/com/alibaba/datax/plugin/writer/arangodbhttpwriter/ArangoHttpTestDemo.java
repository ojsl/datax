package com.alibaba.datax.plugin.writer.arangodbhttpwriter;

import com.alibaba.fastjson.JSON;
import org.apache.http.HttpEntity;
import org.apache.http.ParseException;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class ArangoHttpTestDemo {
    public static void main(String[] args) {
        CredentialsProvider provider = new BasicCredentialsProvider();
        UsernamePasswordCredentials credentials
                = new UsernamePasswordCredentials("test_manager", "test_manager@2020.03");
        provider.setCredentials(AuthScope.ANY, credentials);

        CloseableHttpClient httpClient = HttpClientBuilder.create().setDefaultCredentialsProvider(provider)
                .build();
        HttpPost httpPost = new HttpPost(
                "http://192.168.1.123:38529/_open/auth");
        /*HttpPost httpPost = new HttpPost(
                "http://192.168.1.123:38529/_db/test_datax2/_api/document/money_flow");*/
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("username", "test_manager");
        map.put("password", "test_manager@2020.03");
        String jsonObject = JSON.toJSONString(map);
        StringEntity entity = new StringEntity(jsonObject, "UTF-8");
        httpPost.setEntity(entity);
        httpPost.setHeader("Content-Type", "application/json;charset=utf8");
        // 响应模型
        CloseableHttpResponse response = null;
        try {
            // 由客户端执行(发送)Post请求
            response = httpClient.execute(httpPost);
            // 从响应模型中获取响应实体
            HttpEntity responseEntity = response.getEntity();

            System.out.println("响应状态为:" + response.getStatusLine());
            if (responseEntity != null) {
                System.out.println("响应内容长度为:" + responseEntity.getContentLength());
                //主动设置编码，防止相应出现乱码
                System.out.println("响应内容为:" + EntityUtils.toString(responseEntity, StandardCharsets.UTF_8));

            }
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                // 释放资源
                if (httpClient != null) {
                    httpClient.close();
                }
                if (response != null) {
                    response.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}
