package com.example.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.cat.IndicesResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.elasticsearch.indices.GetIndexResponse;
import co.elastic.clients.elasticsearch.indices.IndexState;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Map;

/**
 * @version 1.0.0
 * @className: IndexTest
 * @description: 索引测试
 * @author: LiJunYi
 * @create: 2022/8/8 10:03
 */
@SpringBootTest
@Slf4j
public class IndexTest
{
    @Autowired
    private ElasticsearchClient elasticsearchClient;

    /**
     * 创建索引
     *
     * @throws IOException ioexception
     */
    @Test
    void createIndex() throws IOException {
        CreateIndexResponse response = elasticsearchClient.indices().create(c -> c.index("products"));
        //响应状态
        boolean acknowledged = response.acknowledged();
        boolean shardsAcknowledged = response.shardsAcknowledged();
        String index = response.index();
        log.info("创建索引状态:{}",acknowledged);
        log.info("已确认的分片:{}",shardsAcknowledged);
        log.info("索引名称:{}",index);
    }

    /**
     * 获取索引
     */
    @Test
    void getIndex() throws IOException {
        // 查看指定索引
        GetIndexResponse getIndexResponse = elasticsearchClient.indices().get(s -> s.index("products"));
        Map<String, IndexState> result = getIndexResponse.result();
        result.forEach((k, v) -> log.info("key = {},value = {}",k ,v));

        // 查看全部索引
        IndicesResponse indicesResponse = elasticsearchClient.cat().indices();
        indicesResponse.valueBody().forEach(
                info -> log.info("health:{}\n status:{} \n uuid:{} \n ",info.health(),info.status(),info.uuid())
        );
    }

    /**
     * 删除索引
     *
     * @throws IOException ioexception
     */
    @Test
    void deleteIndex() throws IOException {
        DeleteIndexResponse deleteIndexResponse = elasticsearchClient.indices().delete(s -> s.index("kit"));
        log.info("删除索引操作结果：{}",deleteIndexResponse.acknowledged());
    }
}
