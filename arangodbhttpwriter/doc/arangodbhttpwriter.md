# DataX ArangoDbHttpWriter 插件文档


------------

## 1 快速介绍

ArangodbHttpWriter实现了Http方式写入Arangodb，支持document、edge类型数据导入。


## 2 功能与限制

ArangodbHttpWriter使用rest api访问arangodb server

1. 支持多种类型数据读取，以json结构组装输入数据

2. 支持批次写入，同步异步写入

3. 支持jwt认证，实现了重试机制

我们暂时不能做到：

1. 除了写入数据外其他操作

## 3 功能说明


### 3.1 配置样例


```
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
                    "name": "streamreader",
                    "parameter": {
                      "print": true
                    }

                },
                "writer": {
                     "name": "arangodbhttpwriter",
                     "parameter": {
                         "url": "http://192.168.2.201:8529/",
                         "type": "edge",
                         "collectionName": "edgetest1",
                         "overwrite": false,
                         "waitForSync": false,
                         "silent": false,
                         "routeNum": 10,
                         "connectNum": 10,
                         "retryNum": 5,
                         "timeout": 60000,
                         "sslVersion": "",
                         "keystorePath": "",
                         "keystorePass": "",
                         "batchSize": 1000,
                         "column": [{
                              "name": "object_key",
                              "type": "String",
                               "index": 0
                         },
                         {
                              "name": "object_key",
                              "type": "String",
                               "index": 0
                         }]
                     }
            }
            }
        ]
    }
}
```


### 3.2 参数说明（各个配置项值前后不允许有空格）

* **url**

	* 描述：arangodb rest url**

	* 必选：是 <br />

	* 默认值：无 <br />
	
* **type**

	* 描述：arangodb 文档类型，可选性edge、document**

	* 必选：是 <br />

	* 默认值：无 <br />

* **collectionName**

	* 描述：arangodb 文档名称**

	* 必选：是 <br />

	* 默认值：无 <br />

* **overwrite**

	* 描述：是否覆盖**

	* 必选：否 <br />

	* 默认值：false <br />
	
* **waitForSync**

	* 描述：arangodb写入方式 同步or异步**

	* 必选：否 <br />

	* 默认值: false <br />
		
* **silent**

	* 描述：是否屏蔽返回详细信息**

	* 必选：否 <br />

	* 默认值: false <br />
		
* **routeNum**

	* 描述：http通道数量 **

	* 必选：否 <br />

	* 默认值: 10 <br />
		
* **connectNum**

	* 描述：http连接数量 **

	* 必选：否 <br />

	* 默认值: false <br />
		
* **retryNum**

	* 描述：重试次数**

	* 必选：否 <br />

	* 默认值: 5 <br />
		
* **timeout**

	* 描述：超时时间**

	* 必选：否 <br />

	* 默认值: 1000 <br />
		
* **sslVersion**

	* 描述：ssl版本**

	* 必选：否 <br />

	* 默认值: 无 <br />
		
* **keystorePath**

	* 描述：ssl认证keystorePath路径**

	* 必选：否 <br />

	* 默认值: 无 <br />
		
* **keystorePass**

	* 描述：ssl认证密码**

	* 必选：否 <br />

	* 默认值: 无 <br />
		
* **batchSize**

	* 描述：每次提交的批次数量**

	* 必选：否 <br />

	* 默认值: 1000 <br />
	
column内参数：
		
* **name**

	* 描述：字段名称**

	* 必选：是 <br />

	* 默认值: 无 <br />
		
* **type**

	* 描述：字段类型**

	* 必选：是 <br />

	* 默认值: 无 <br />
		
* **index**

	* 描述：取数据源中字段的位置**

	* 必选：是 <br />

	* 默认值: 无 <br />
	
	
### 3.3 类型转换

arangodb http接口数据输入格式为json。ArangoDbHttpWriter提供了类型转换的建议表如下：

| DataX 内部类型| arangodb 数据类型    |
| -------- | -----  |
| Long     |BIGINT|
| Double   |DOUBLE|
| String   |String|
| Boolean  |BOOLEAN|
| Date     |Date|
| Bytes    |Bytes|


