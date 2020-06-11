# DataX ArangoDbHttpWriter 插件文档


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
| 


