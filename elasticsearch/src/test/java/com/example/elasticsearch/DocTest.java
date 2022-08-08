package com.example.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import com.example.elasticsearch.model.User;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @version 1.0.0
 * @className: DocTest
 * @description: 文档操作测试
 * @author: LiJunYi
 * @create: 2022/8/8 10:19
 */
@SpringBootTest
@Slf4j
public class DocTest
{
    @Autowired
    private ElasticsearchClient elasticsearchClient;

    /**
     * 添加一个文档
     * https://www.elastic.co/guide/en/elasticsearch/client/java-api-client/8.3/indexing.html#indexing
     * @throws IOException ioexception
     */
    @Test
    void addOneDocument () throws IOException
    {
        // 1、using the fluent DSL
        User user = new User("1","王五",28,"男");
        IndexResponse indexResponse = elasticsearchClient.index(s ->
                // 索引
                s.index("users")
                        // ID
                        .id(user.getId())
                        // 文档
                        .document(user)
        );
        log.info("result:{}",indexResponse.result().jsonValue());
        // 2、You can also assign objects created with the DSL to variables. Java API Client classes have a static of() method for this, that creates an object with the DSL syntax.
        IndexRequest<User> request = IndexRequest.of(i -> i
                .index("users")
                .id(user.getId())
                .document(user));
        IndexResponse response = elasticsearchClient.index(request);
        log.info("Indexed with version " + response.version());
        // 3、Using classic builders
        IndexRequest.Builder<User> indexReqBuilder = new IndexRequest.Builder<>();
        indexReqBuilder.index("users");
        indexReqBuilder.id(user.getId());
        indexReqBuilder.document(user);
        IndexResponse responseTwo = elasticsearchClient.index(indexReqBuilder.build());
        log.info("Indexed with version " + responseTwo.version());
    }

    /**
     * 更新文档
     *
     * @throws IOException ioexception
     */
    @Test
    void updateDocument () throws IOException
    {
        // 构建需要修改的内容，这里使用了Map
        Map<String, Object> map = new HashMap<>();
        map.put("name", "liuyife");
        // 构建修改文档的请求
        UpdateResponse<Test> response = elasticsearchClient.update(e -> e
                .index("users")
                .id("33")
                .doc(map),
                Test.class
        );
        // 打印请求结果
        log.info(String.valueOf(response.result()));
    }

    /**
     * 批量添加文档
     *
     * @throws IOException ioexception
     */
    @Test
    void batchAddDocument () throws IOException
    {
        List<User> users = new ArrayList<>();
        users.add(new User("1","赵四",20,"男"));
        users.add(new User("2","阿旺",25,"男"));
        users.add(new User("3","刘菲",22,"女"));
        users.add(new User("4","冬梅",20,"女"));

        List<BulkOperation> bulkOperations = new ArrayList<>();
        users.forEach(u ->
                        bulkOperations.add(BulkOperation.of(b ->
                                b.index(
                                        c ->
                                                c.id(u.getId()).document(u)
                                )))
                );
        BulkResponse bulkResponse = elasticsearchClient.bulk(s -> s.index("users").operations(bulkOperations));
        bulkResponse.items().forEach(i ->
               log.info("i = {}" , i.result()));
        log.error("bulkResponse.errors() = {}" , bulkResponse.errors());

        // 2、use BulkRequest
        BulkRequest.Builder br = new BulkRequest.Builder();
        for (User user : users) {
            br.operations(op -> op
                    .index(idx -> idx
                            .index("users")
                            .id(user.getId())
                            .document(user)));
        }
        BulkResponse result = elasticsearchClient.bulk(br.build());
        // Log errors, if any
        if (result.errors()) {
            log.error("Bulk had errors");
            for (BulkResponseItem item: result.items()) {
                if (item.error() != null) {
                    log.error(item.error().reason());
                }
            }
        }
    }

    /**
     * 获取文档
     * https://www.elastic.co/guide/en/elasticsearch/client/java-api-client/8.3/reading.html#reading
     * @throws IOException ioexception
     */
    @Test
    void getDocument () throws IOException
    {
        // co.elastic.clients.elasticsearch.core.get.GetResult<TDocument>
        GetResponse<User> getResponse = elasticsearchClient.get(s -> s.index("users").id("e051445c-ae8c-47ef-ab18-97b34025d49a"),User.class);
        log.info("getResponse:{}",getResponse.source());

        // Reading a domain object
        if (getResponse.found())
        {
            User user = getResponse.source();
            assert user != null;
            log.info("user name={}",user.getName());
        }

        // Reading raw JSON
        // if (getResponse.found())
        // {
        //    ObjectNode json = getResponse.source();
        //    String name = json.get("name").asText();
        //    log.info("Product name " + name);
        // }

        // 判断文档是否存在
        BooleanResponse booleanResponse = elasticsearchClient.exists(s -> s.index("users").id("e051445c-ae8c-47ef-ab18-97b34025d49a"));
        log.info("判断Document是否存在:{}",booleanResponse.value());
    }

    /**
     * 删除文档
     *
     * @throws IOException ioexception
     */
    @Test
    void deleteDocument () throws IOException
    {
        DeleteResponse deleteResponse = elasticsearchClient.delete(s -> s.index("users").id("e051445c-ae8c-47ef-ab18-97b34025d49a"));
        log.info("删除文档操作结果:{}",deleteResponse.result());
    }

    /**
     * 批量删除文档
     *
     * @throws IOException ioexception
     */
    @Test
    void batchDeleteDocument () throws IOException
    {
        // 1、use BulkOperation
        List<String> list = new ArrayList<>();
        list.add("1");
        list.add("2");
        list.add("3");
        list.add("4");
        List<BulkOperation> bulkOperations = new ArrayList<>();
        list.forEach(a ->
                bulkOperations.add(BulkOperation.of(b ->
                        b.delete(c -> c.id(a))
                ))
        );
        BulkResponse bulkResponse = elasticsearchClient.bulk(a -> a.index("users").operations(bulkOperations));
        bulkResponse.items().forEach(a ->
                log.info("result = {}" , a.result()));
        log.error("bulkResponse.errors() = {}" , bulkResponse.errors());

        // 2、use BulkRequest
        BulkRequest.Builder br = new BulkRequest.Builder();
        for (String s : list) {
            br.operations(op -> op
                    .delete(c -> c.id(s)));
        }
        BulkResponse bulkResponseTwo = elasticsearchClient.bulk(br.build());
        bulkResponseTwo.items().forEach(a ->
                log.info("result = {}" , a.result()));
        log.error("bulkResponse.errors() = {}" , bulkResponseTwo.errors());
    }
}
