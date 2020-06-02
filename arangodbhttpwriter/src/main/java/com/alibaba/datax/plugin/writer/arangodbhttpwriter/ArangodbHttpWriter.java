package com.alibaba.datax.plugin.writer.arangodbhttpwriter;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.*;

public class ArangodbHttpWriter extends Writer {
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originConfig = null;
        private String url;
        private String type;
        private String collectionName;

        @Override
        public void init() {
            this.originConfig = this.getPluginJobConf();
            this.url = this.originConfig.getString(Key.URL);
            this.type = this.originConfig.getString(Key.TYPE);
            this.collectionName = this.originConfig.getString(Key.COLLECTION_NAME);

        }

        @Override
        public void prepare() {
            if(url.isEmpty()){
                throw DataXException.asDataXException(ArangodbHttpWriterErrorCode.
                        URL_EMPTY_ERROR,"url配置值为空，请检查！");
            }
            if(type.isEmpty()){
                throw DataXException.asDataXException(ArangodbHttpWriterErrorCode.
                        TYPE_EMPTY_ERROR,"type配置值为空，请检查！");
            }
            if(collectionName.isEmpty()){
                throw DataXException.asDataXException(ArangodbHttpWriterErrorCode.
                        COLLECTION_NAME_EMPTY_ERROR,"collectionName配置值为空，请检查！");
            }
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> splitResultConfigs = new ArrayList<Configuration>();
            for (int j = 0; j < mandatoryNumber; j++) {
                splitResultConfigs.add(originConfig.clone());
            }
            return splitResultConfigs;
        }


    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerSliceConfig;

        private String url;
        private String jwturl;
        private String type;
        private String collectionName;
        private String database;
        private String username;
        private String password;
        private String authJsonString;
        private String jwtStr;
        private boolean overWrite;
        private boolean silent;
        private boolean waitForSync;
        private int routeNum;
        private int connectNum;
        private int retryNum;
        private int timeout;
        private boolean ssl;
        private String sslVersion;
        private String keystorePath;
        private String keystorePass;
        private String column;
        private int batchSize;
        private JSONArray jscolumns;
        private List<String> parms = new ArrayList<>();

        private Header[] headers = null;
        private HCB hcb = null;
        private HttpClient client = null;
        private static final boolean IS_DEBUG = LOG.isDebugEnabled();


        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.column = this.writerSliceConfig.getString(Key.COLUMN);
            this.batchSize = this.writerSliceConfig.getInt(Key.BATCH_SIZE,10);
            this.url = this.writerSliceConfig.getString(Key.URL);
            this.type = this.writerSliceConfig.getString(Key.TYPE);
            this.collectionName = this.writerSliceConfig.getString(Key.COLLECTION_NAME);
            this.database = this.writerSliceConfig.getString(Key.DATABASE,"_system");
            this.overWrite = this.writerSliceConfig.getBool(Key.OVERWRITE,false);
            parms.add("overWrite="+this.overWrite);
            this.silent = this.writerSliceConfig.getBool(Key.SILENT,false);
            parms.add("silent="+this.silent);
            this.waitForSync = this.writerSliceConfig.getBool(Key.WAITFORSYNC,true);
            parms.add("waitForSync="+this.waitForSync);
            this.routeNum = this.writerSliceConfig.getInt(Key.ROUTE_NUM,10);
            this.connectNum = this.writerSliceConfig.getInt(Key.CONNECT_NUM,10);
            this.retryNum = this.writerSliceConfig.getInt(Key.RETRY_NUM,5);
            this.timeout = this.writerSliceConfig.getInt(Key.TIMEOUT,1000);
            this.ssl = this.url.toLowerCase().contains("https");
            this.sslVersion = this.writerSliceConfig.getString(Key.SSL_VERSION,"TLSv1.2");
            this.keystorePath = this.writerSliceConfig.getString(Key.KEYSTORE_PATH);
            this.keystorePass = this.writerSliceConfig.getString(Key.KEYSTORE_PASS);
            this.username = this.writerSliceConfig.getString(Key.USERNAME);
            this.password = this.writerSliceConfig.getString(Key.PASSWORD);

            this.jwturl = assembJwtUrl(url);

            this.url = assembUrl(parms,this.url,this.type,this.collectionName);

            this.jscolumns = JSON.parseArray(column);

            this.headers = HttpHeader.custom()
                    .userAgent("javacl")
                    .accept("application/json")
                    //.other("customer", "自定义")
                    .build();

            try{
                this.hcb = HCB.custom()
                        .timeout(this.timeout) //超时
                        .pool(this.routeNum*this.connectNum, this.connectNum) //启用连接池，每个路由最大创建10个链接，总连接数限制为100个
                        //.sslpv(SSLs.SSLProtocolVersion.TLSv1_2) 	//设置ssl版本号，默认SSLv3，也可以调用sslpv("TLSv1.2")
                        //.ssl()  	  	//https，支持自定义ssl证书路径和密码，ssl(String keyStorePath, String keyStorepass)
                        .retry(this.retryNum)		//重试5次
                ;
            }catch (Exception e){
                throw DataXException.asDataXException(ArangodbHttpWriterErrorCode.CREATE_HCB_ERROR, e);
            }
            this.client = hcb.build();

            if(!username.equalsIgnoreCase("")){
                Map<String, Object> map = new HashMap<String, Object>();
                map.put("username", this.username);
                map.put("password", this.password);
                this.authJsonString = JSON.toJSONString(map);

                getJwtStr(0);
            }
        }

        @Override
        public void prepare() {
            if(jscolumns == null || jscolumns.size() == 0){
                throw DataXException.asDataXException(ArangodbHttpWriterErrorCode.COLUMN_ERROR, "解析column异常，请检查配置");
            }

        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            LOG.info("begin do write...");
            try {
                Record record = null;
                List<Record> buffer = new ArrayList<Record>(this.batchSize);
                int bufferBytes = 0;
                while ((record = lineReceiver.getFromReader()) != null) {
                    // 校验列数量是否符合预期
                    /*if (record.getColumnNumber() != jscolumns.size()) {
                        throw DataXException.asDataXException(ArangodbHttpWriterErrorCode.COLUMN_NOT_MATCH_ERROR,
                                "数据源给出的列数量[" + record.getColumnNumber() + "]与您配置中的列数量[" + jscolumns.size() +
                                        "]不同, 请检查您的配置 或者 联系 Arangodb 管理员.");
                    }*/
                    buffer.add(record);
                    if (buffer.size() >= batchSize) {
                        doBatchInsert(buffer,super.getTaskPluginCollector());
                        buffer.clear();
                    }


                }
                doBatchInsert(buffer,super.getTaskPluginCollector());
            }catch (Throwable e){
                throw DataXException.asDataXException(ArangodbHttpWriterErrorCode.ARANGODB_PUT_ERROR, e);
            }
            LOG.info("end do write");
        }

        protected void doBatchInsert(List<Record> buffer, TaskPluginCollector taskPluginCollector){
            String jastr =  getBatch(buffer,taskPluginCollector);
            HttpResult respResult = executePost(jastr,this.url);
            int resultCode = respResult.getStatusCode();
            int num = 0;
            switch (resultCode){
                case 200:
                case 201:
                case 202:
                    break;

                case 400:
                    throw DataXException.asDataXException(ArangodbHttpWriterErrorCode.FORMAT_ERROR,
                            respResult.getResult()+
                                    "\\nurl:"+this.url+"\\njson:"+jastr);
                case 404:
                    throw DataXException.asDataXException(ArangodbHttpWriterErrorCode.DOCUMENT_NOT_FOUND_ERROR,
                            respResult.getResult()+
                                    "\\nurl:"+this.url+"\\njson:"+jastr);
                case 412:
                    throw DataXException.asDataXException(ArangodbHttpWriterErrorCode.VIOLATION_CONSTRAINT_ERROR,
                            respResult.getResult()+
                                    "\\nurl:"+this.url+"\\njson:"+jastr);
                case 401:
                    num++;
                    getJwtStr(num);
                    doBatchInsert(buffer,taskPluginCollector);
                    break;
                default:
                    throw DataXException.asDataXException(ArangodbHttpWriterErrorCode.HTTP_ERROR, "http接口返回异常！");

            }
        }



        protected String getBatch(List<Record> buffer, TaskPluginCollector taskPluginCollector){
            JSONArray jsonArray = new JSONArray();
            for (Record record : buffer) {
                int recordLength = record.getColumnNumber();

                try {
                    if (0 != recordLength) {
                        JSONObject jsre = new JSONObject();
                        for(int i= 0;i<jscolumns.size();i++){

                            JSONObject jo = jscolumns.getJSONObject(i);
                            int index;
                            if(jo.containsKey(Key.INDEX)){
                                index = jo.getIntValue(Key.INDEX);
                            }else {
                                taskPluginCollector.collectDirtyRecord(record,
                                        "当前配置缺失index配置项！json:"+jo.toJSONString());
                                break;
                            }

                            if(jo.containsKey("notnull") && jo.getBoolean("notnull") && (record.getColumn(index) == null
                                    ||record.getColumn(index).asString().equalsIgnoreCase(""))){
                                throw DataXException.asDataXException(ArangodbHttpWriterErrorCode.COLUMN_NOT_NULL_ERROR,
                                        "配置中的index[" + index + "]所在字段为空，不符合配置非空要求。");
                            }
                            if(index>record.getColumnNumber()){
                                throw DataXException.asDataXException(ArangodbHttpWriterErrorCode.COLUMN_NOT_MATCH_ERROR,
                                        "配置中的index[" + index + "]超出数据源给出的列数量[" + record.getColumnNumber() + "], 请检查您的配置.");
                            }
                            switch (jo.getString(Key.TYPE).toLowerCase()) {
                                case "string":
                                    jsre.put(jo.getString("name"),record.getColumn(index).asString());
                                    break;
                                case "bool":
                                    jsre.put(jo.getString("name"),record.getColumn(index).asBoolean());
                                    break;
                                case "double":
                                    jsre.put(jo.getString("name"),record.getColumn(index).asDouble());
                                    break;
                                case "long":
                                    jsre.put(jo.getString("name"),record.getColumn(index).asBigInteger());
                                    break;
                                case "bytes":
                                    jsre.put(jo.getString("name"),record.getColumn(index).asBytes());
                                    break;
                                case "date":
                                    jsre.put(jo.getString("name"),record.getColumn(index).asDate());
                                    break;
                                default:
                                    throw DataXException
                                            .asDataXException(
                                                    ArangodbHttpWriterErrorCode.COLUMN_ERROR,
                                                    String.format(
                                                            "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库读取这种字段类型. 字段名:[%s], 字段名称:[%s]. 请尝试使用数据库函数将其转换datax支持的类型 或者不同步该字段 .",
                                                            jo.getString("name"),
                                                            jo.getString("type")));
                            }
                        }
                        if(this.type.equalsIgnoreCase("edge")){
                            if(!jsre.containsKey("_from")){
                                taskPluginCollector.collectDirtyRecord(record, "当前记录没有_from字段，不符合edge类型插入要求。");
                                continue;
                            }else if(!jsre.containsKey("_to")){
                                taskPluginCollector.collectDirtyRecord(record, "当前记录没有_to字段，不符合edge类型插入要求。");
                                continue;
                            }
                        }
                        jsonArray.add(jsre);
                    }

                }catch (Exception e){
                    if (IS_DEBUG) {
                        LOG.debug("read data " + record.toString()
                                + " occur exception:", e);
                    }
                    //TODO 这里识别为脏数据靠谱吗？
                    taskPluginCollector.collectDirtyRecord(record, e);
                    if (e instanceof DataXException) {
                        throw (DataXException) e;
                    }
                }
            }
            return jsonArray.toJSONString();
            //executePost(jsonArray.toJSONString());
        }

        protected HttpResult executePost(String str,String url){
            HttpConfig config = HttpConfig.custom()
                    .headers(this.headers)	//设置headers，不需要时则无需设置
                    .url(url)	          //设置请求的url
                    //.map(map)	          //设置请求参数，没有则无需设置
                    .encoding("utf-8") //设置请求和返回编码，默认就是Charset.defaultCharset()
                    .client(this.client)    //如果只是简单使用，无需设置，会自动获取默认的一个client对象
                    //.inenc("utf-8")  //设置请求编码，如果请求返回一直，不需要再单独设置
                    //.inenc("utf-8")	//设置返回编码，如果请求返回一直，不需要再单独设置
                    .json(str)
                    //json方式请求的话，就不用设置map方法，当然二者可以共用。
                    //.context(HttpCookies.custom().getContext()) //设置cookie，用于完成携带cookie的操作
                    //.out(new FileOutputStream("保存地址"))       //下载的话，设置这个方法,否则不要设置
                    //.files(new String[]{"d:/1.txt","d:/2.txt"}) //上传的话，传递文件路径，一般还需map配置，设置服务器保存路径
                    ;
            try{
                HttpResult respResult = HttpClientUtil.sendAndGetResp(config.method(HttpMethods.POST));

                return respResult;
            }catch (HttpProcessException e){
                throw DataXException.asDataXException(ArangodbHttpWriterErrorCode.HTTP_ERROR,
                        e.getMessage());
            }
        }

        protected String assembUrl(List<String> parms,String url,String type,String collectionName){
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(url);
            switch (type){
                case "document":
                case "edge":
                    if(url.endsWith("/")){
                        stringBuilder.append("_db/");
                    }else {
                        stringBuilder.append("/_db/");
                    }
                    stringBuilder.append(this.database);
                    stringBuilder.append("/_api/document/");
                    stringBuilder.append(collectionName);
                    break;
                default:
                    throw DataXException.asDataXException(ArangodbHttpWriterErrorCode.TYPE_ERROR,
                            "配置值type不符合规范，请检查。");
            }
            if(parms.size()>0){

                stringBuilder.append("?");
                for(int i = 0;i<parms.size();i++){
                    if(i>0){
                        stringBuilder.append("&");
                    }
                    stringBuilder.append(parms.get(i));
                }
                return stringBuilder.toString();
            }else {
                return url;
            }

        }

        protected String assembJwtUrl(String url){
            if(url.endsWith("/")){
                url +="_open/auth";
            }else {
                url +="/_open/auth";
            }
            return url;
        }

        protected void getJwtStr(int num) {
                num++;
            LOG.info("尝试jwt认证。authstr:"+this.authJsonString+"。JwtUrl:"+this.jwturl);
                HttpResult httpResult = executePost(this.authJsonString,this.jwturl);
                int resultCode = httpResult.getStatusCode();
                switch (resultCode){
                    case 200:
                        String result = httpResult.getResult();
                        JSONObject jsonObject = JSON.parseObject(result);
                        if(result !=null && result.length()>0 &&
                                jsonObject!=null && jsonObject.size()>0){
                            this.jwtStr = "bearer "+jsonObject.getString("jwt");
                            headers = HttpHeader.custom()
                                    .userAgent("javacl")
                                    .accept("application/json")
                                    .authorization(jwtStr)
                                    //.other("customer", "自定义")
                                    .build();

                            LOG.info("JWT认证成功，JWTSTR:"+jwtStr);
                        }
                        else {
                            throw DataXException.asDataXException(ArangodbHttpWriterErrorCode.AUTH_PARSE_ERROR,
                                    "result:"+result);
                        }
                        break;
                    case 401:
                        if(num<this.retryNum){
                            getJwtStr(num);
                        }else {
                            throw DataXException.asDataXException(ArangodbHttpWriterErrorCode.AUTH_FAISE_ERROR,
                                    "权限认证失败，请检查配置");
                        }
                        break;
                    default:
                        throw DataXException.asDataXException(ArangodbHttpWriterErrorCode.AUTH_RESULT_ERROR,
                                "权限认证返回异常");
                }

        }




        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }
    }

}
