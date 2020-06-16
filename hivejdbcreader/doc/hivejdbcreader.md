# DataX HiveJdecReader 插件文档


------------

## 1 快速介绍

HiveJdbcReader实现了从jdbc获取数据的实现，
**HdfsReader需要Jdk1.7及以上版本的支持。**


## 2 功能与限制

HiveJdbcReader使用JDBC驱动访问hiveserver2获取数据

1. 支持多种类型数据读取(使用String表示)，支持列常量

2. 支持递归读取、支持正则表达式（"*"和"?"）。

3. 支持kerberos认证（注意：如果用户需要进行kerberos认证，那么用户使用的Hadoop集群版本需要和hdfsreader的Hadoop版本保持一致，如果高于hdfsreader的Hadoop版本，不保证kerberos认证有效）

我们暂时不能做到：

1. 当前版本不能并发读取。
2. 目前还不支持多实例hiveserver2;



## 3 功能说明


### 3.1 配置样例

```json
{
    "job": {
        "setting": {
            "speed": {
                "channel": 3
            }
        },
        "content": [
            {
                "reader": {
                    "name": "hivejdbcreader",
                    "parameter": {
                        "hiveUrl": "jdbc:hive2://192.168.1.131:10000/",
                        "executeSql": "select object_key,communities from bigdata_test.tv_user limit 10000",
                        "username": "",
                        "password": "",
                        "haveKerberos": false,
                        "kerberosPrincipal": "",
                        "kerberosKeytabFilePath": "",
                        "batchSize": 1000,
                        "krb5conf": "",
                        "hivesite": "",
                        "coresite": ""
                    }

                },
                "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "print": true
                    }
                }
            }
        ]
    }
}
```

### 3.2 参数说明（各个配置项值前后不允许有空格）

* **hiveUrl**

	* 描述：hive jdbc访问url。**

	* 必选：是 <br />

	* 默认值：无 <br />

* **username**

	* jdbc用户名 <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **password**

	* 描述：jdbc密码 <br />

	* 必选：否 <br />

	* 默认值：无 <br />


* **haveKerberos**

	* 描述：是否有Kerberos认证，默认false<br />
          
    例如如果用户配置true，则配置项kerberosKeytabFilePath，kerberosPrincipal为必填。

	* 必选：否 <br />

	* 默认值：false <br />

* **kerberosKeytabFilePath**

	* 描述：Kerberos认证 keytab文件路径，绝对路径 <br />

	* 必选：否 <br />

	* 默认值：无 <br />


* **kerberosPrincipal**

	* 描述：kerberos认证principal属性。<br />

 	* 必选：否 <br />

 	* 默认值：无 <br />


* **batchSize**

	* 描述：每次从jdbc驱动获取的批次数量。

 	* 必选：否 <br />

 	* 默认值：1000 <br />

* **executeSql**

	* 描述：需要提交的sql

 	* 必选：是 <br />
 
 	* 默认值：无 <br />

* **krb5conf**

	* 描述：Kerberos认证 krb5conf文件路径，绝对路径<br />

 	* 必选：否 <br />
 
 	* 默认值：无 <br />

* **hivesite**

	* 描述：hive-site.xml文件路径，绝对路径 <br />

 	* 必选：haveKerberos 为true必选 <br />
 
 	* 默认值：无 <br />

* **coresite**

	* 描述：core-site.xml文件路径，绝对路径 <br />

 	* 必选：haveKerberos 为true必选 <br />
 
 	* 默认值：无 <br />
	
* **hadoopConfig**

	* 描述：hadoopConfig里可以配置与Hadoop相关的一些高级参数，比如HA的配置。<br />

		```json
		"hadoopConfig":{
		        "dfs.nameservices": "testDfs",
		        "dfs.ha.namenodes.testDfs": "namenode1,namenode2",
		        "dfs.namenode.rpc-address.aliDfs.namenode1": "",
		        "dfs.namenode.rpc-address.aliDfs.namenode2": "",
		        "dfs.client.failover.proxy.provider.testDfs": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
		}
		```

	* 必选：否 <br />
 
 	* 默认值：无 <br />
     

### 3.3 类型转换

数据源的数据类型由sql result metadata中获取。HiveJdbcReader提供了类型转换的建议表如下：

| DataX 内部类型| Hive表 数据类型    |
| -------- | -----  |
| Long     |TINYINT,SMALLINT,INT,BIGINT|
| Double   |FLOAT,DOUBLE|
| String   |String,CHAR,VARCHAR,BINARY|
| Boolean  |BOOLEAN|
| Date     |Date,TIMESTAMP|

未支持的类型：STRUCT,MAP,ARRAY,UNION


其中：

* Long是指Hdfs文件文本中使用整形的字符串表示形式，例如"123456789"。
* Double是指Hdfs文件文本中使用Double的字符串表示形式，例如"3.1415"。
* Boolean是指Hdfs文件文本中使用Boolean的字符串表示形式，例如"true"、"false"。不区分大小写。
* Date是指Hdfs文件文本中使用Date的字符串表示形式，例如"2014-12-31"。

特别提醒：

* Hive支持的数据类型TIMESTAMP可以精确到纳秒级别，所以textfile、orcfile中TIMESTAMP存放的数据类似于"2015-08-21 22:40:47.397898389"，如果转换的类型配置为DataX的Date，转换之后会导致纳秒部分丢失，所以如果需要保留纳秒部分的数据，请配置转换类型为DataX的String类型。



## 4 性能报告



## 5 约束限制

略

## 6 FAQ


