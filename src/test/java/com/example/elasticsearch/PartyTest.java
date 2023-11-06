package com.example.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.aggregations.*;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

/**
 * @version 1.0.0
 * @className: SearchTest
 * @description: 聚合查询测试
 * @author: LiJunYi
 * @create: 2022/8/8 11:04
 */
@SpringBootTest
@Slf4j
public class PartyTest
{
    @Autowired
    private ElasticsearchClient elasticsearchClient;

    /**
     * 获取最大年龄用户测试
     */
    @Test
    void getMaxAgeUserTest() throws IOException {
        SearchResponse<Void> response = elasticsearchClient.search(b -> b
                        .index("users")
                        .size(0)
                        .aggregations("maxAge", a -> a
                                .max(MaxAggregation.of(s -> s
                                        .field("age"))
                                )
                        ),
                Void.class
        );
        MaxAggregate maxAge = response.aggregations()
                .get("maxAge")
                .max();
        log.info("maxAge.value:{}",maxAge.value());
    }

    /**
     * 年龄分组测试
     *
     * @throws IOException ioexception
     */
    @Test
    void groupByAgeTest() throws IOException {
        SearchResponse<Void> response = elasticsearchClient.search(b -> b
                        .index("users")
                        .size(0)
                        .aggregations("groupName", a -> a
                                .terms(TermsAggregation.of(s -> s
                                        .field("age")))
                        ),
                Void.class
        );
        LongTermsAggregate longTermsAggregate = response.aggregations()
                .get("groupName")
                .lterms();
        log.info("multiTermsAggregate:{}",longTermsAggregate.buckets());
    }

    /**
     * 性别分组测试
     *
     * @throws IOException ioexception
     */
    @Test
    void groupBySexTest() throws IOException {
        SearchResponse<Void> response = elasticsearchClient.search(b -> b
                        .index("users")
                        .size(0)
                        .aggregations("groupSex", a -> a
                                .terms(TermsAggregation.of(s -> s
                                        .field("sex.keyword")))
                        ),
                Void.class
        );
        StringTermsAggregate stringTermsAggregate = response.aggregations()
                .get("groupSex")
                .sterms();
        log.info("stringTermsAggregate:{}",stringTermsAggregate.buckets());
    }
}
