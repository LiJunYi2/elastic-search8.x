# elasticSearch8.x

#### 介绍
 **elasticSearch8.x（v8.3.3）最新版的笔记记录** 

 **elasticSearch系列笔记博客直达** ：[elasticSearch](https://lijunyi.xyz/docs/middleware/elasticSearch/abstract.html)

#### 学习教程

1.  clone项目，根据笔记内容进行一步步学习深入

#### es集群引入方式
1. `application`内容改为：

```yaml
elasticsearch:
  hosts: 10.4.122.74:9200,10.1.134.73:9200,10.2.156.64:9200
  username: elastic
  password: Mv23435@al
```

2. 客户端改为 `EsClientConfig`
