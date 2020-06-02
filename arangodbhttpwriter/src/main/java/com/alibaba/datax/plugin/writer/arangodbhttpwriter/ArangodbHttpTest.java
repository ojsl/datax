package com.alibaba.datax.plugin.writer.arangodbhttpwriter;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.arronlong.httpclientutil.HttpClientUtil;
import com.arronlong.httpclientutil.builder.HCB;
import com.arronlong.httpclientutil.common.HttpConfig;
import com.arronlong.httpclientutil.common.HttpHeader;
import com.arronlong.httpclientutil.common.HttpMethods;
import com.arronlong.httpclientutil.common.HttpResult;
import com.arronlong.httpclientutil.exception.HttpProcessException;
import org.apache.http.Header;
import org.apache.http.client.HttpClient;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

public class ArangodbHttpTest {
    public static void main(String[] args) throws HttpProcessException, FileNotFoundException {
        //String url = "http://192.168.1.36:8529/_api/document/female/alice";
        String url = "http://192.168.1.123:38529/_db/hive_test/_api/document/te_user";
        url = "http://192.168.1.123:38529/_open/auth";
        //最简单的使用：
        //String html = HttpClientUtil.get(HttpConfig.custom().url(url));
        //System.out.println(html);

        //---------------------------------
        //			【详细说明】
        //--------------------------------

        String jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9." +
                "eyJpYXQiOjEuNTkwNzQwMTE2NjQzMTkzZSs2LCJleHA" +
                "iOjE1OTMzMzIxMTYsImlzcyI6ImFyYW5nb2RiIiwicHJ" +
                "lZmVycmVkX3VzZXJuYW1lIjoidGVzdF9tYW5hZ2VyIn0=." +
                "UKQWjYFw1GMEpi8lrI83YYqfVStwHD5kWJdw2ksW5z8=";

        //插件式配置Header（各种header信息、自定义header）
        Header[] headers = HttpHeader.custom()
                .userAgent("javacl")
                //.authorization("bearer "+jwt)
                

                .accept("application/json")
                //.other("customer", "自定义")
                .build();

        //插件式配置生成HttpClient时所需参数（超时、连接池、ssl、重试）
        HCB hcb = HCB.custom()
                .timeout(1000) //超时

                .pool(100, 10) //启用连接池，每个路由最大创建10个链接，总连接数限制为100个
                //.sslpv(SSLs.SSLProtocolVersion.TLSv1_2) 	//设置ssl版本号，默认SSLv3，也可以调用sslpv("TLSv1.2")
                //.ssl()  	  	//https，支持自定义ssl证书路径和密码，ssl(String keyStorePath, String keyStorepass)
                .retry(5)		//重试5次
                ;

        HttpClient client = hcb.build();

        Map<String, Object> map = new HashMap<String, Object>();
        map.put("username", "test_manager");
        map.put("password", "test_manager@2020.03");
        String jsonObject = JSON.toJSONString(map);
        System.out.println(jsonObject);

        //插件式配置请求参数（网址、请求参数、编码、client）
        HttpConfig config = HttpConfig.custom()
                .headers(headers)	//设置headers，不需要时则无需设置
                .url(url)	          //设置请求的url
                //.map(map)	          //设置请求参数，没有则无需设置
                .encoding("utf-8") //设置请求和返回编码，默认就是Charset.defaultCharset()
                .client(client)    //如果只是简单使用，无需设置，会自动获取默认的一个client对象
                //.inenc("utf-8")  //设置请求编码，如果请求返回一直，不需要再单独设置
                //.inenc("utf-8")	//设置返回编码，如果请求返回一直，不需要再单独设置
                .json(jsonObject)                          //json方式请求的话，就不用设置map方法，当然二者可以共用。
                //.context(HttpCookies.custom().getContext()) //设置cookie，用于完成携带cookie的操作
                //.out(new FileOutputStream("保存地址"))       //下载的话，设置这个方法,否则不要设置
                //.files(new String[]{"d:/1.txt","d:/2.txt"}) //上传的话，传递文件路径，一般还需map配置，设置服务器保存路径
                ;


        ////使用方式：
        //String result1 = HttpClientUtil.get(config);     //get请求
        //String result2 = HttpClientUtil.send(config.method(HttpMethods.POST));    //post请求

        //System.out.println(result2);
        //System.out.println(result2);

        //HttpClientUtil.down(config);                   //下载，需要调用config.out(fileOutputStream对象)
        //HttpClientUtil.upload(config);                 //上传，需要调用config.files(文件路径数组)

        //如果指向看是否访问正常
        //String result3 = HttpClientUtil.head(config); // 返回Http协议号+状态码
        //int statusCode = HttpClientUtil.status(config);//返回状态码

        //[新增方法]sendAndGetResp，可以返回原生的HttpResponse对象，
        //同时返回常用的几类对象：result、header、StatusLine、StatusCode
        HttpResult respResult = HttpClientUtil.sendAndGetResp(config.method(HttpMethods.POST));
        System.out.println("返回结果：\n"+respResult.getResult());
        System.out.println("返回resp-header："+respResult.getRespHeaders());//可以遍历
        System.out.println("返回具体resp-header："+respResult.getHeaders("Date"));
        System.out.println("返回StatusLine对象："+respResult.getStatusLine());
        System.out.println("返回StatusCode："+respResult.getStatusCode());
        System.out.println("返回HttpResponse对象）（可自行处理）："+respResult.getResp());
    }
}
