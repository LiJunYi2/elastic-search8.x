package com.example.elasticsearch;

import cn.hutool.json.JSONUtil;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.SearchTemplateResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import co.elastic.clients.json.JsonData;
import com.example.elasticsearch.model.User;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.*;

/**
 * @version 1.0.0
 * @className: SearchTest
 * @description: 查询测试
 * @author: LiJunYi
 * @create: 2022/8/8 11:04
 */
@SpringBootTest
@Slf4j
public class SearchTest
{
    @Autowired
    private ElasticsearchClient elasticsearchClient;

    /**
     * 批量添加文档
     *
     * @throws IOException ioexception
     */
    @Test
    void batchAddDocument () throws IOException
    {
        List<User> users = new ArrayList<>();
        users.add(new User("11","zhaosi",20,"男"));
        users.add(new User("22","awang",25,"男"));
        users.add(new User("33","liuyifei",22,"女"));
        users.add(new User("44","dongmei",20,"女"));
        users.add(new User("55","zhangya",30,"女"));
        users.add(new User("66","liuyihu",32,"男"));

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
     * 简单的搜索查询
     *
     * @throws IOException ioexception
     */
    @Test
    void searchOne() throws IOException {
        String searchText = "liuyihu";
        SearchResponse<User> response = elasticsearchClient.search(s -> s
                // 我们要搜索的索引的名称
                .index("users")
                // 搜索请求的查询部分（搜索请求也可以有其他组件，如聚合）
                .query(q -> q
                        // 在众多可用的查询变体中选择一个。我们在这里选择匹配查询（全文搜索）
                        .match(t -> t
                                // name配置匹配查询：我们在字段中搜索一个词
                                .field("name")
                                .query(searchText)
                        )
                ),
                // 匹配文档的目标类
                User.class
        );
        TotalHits total = response.hits().total();
        boolean isExactResult = total.relation() == TotalHitsRelation.Eq;

        if (isExactResult) {
            log.info("There are " + total.value() + " results");
        } else {
            log.info("There are more than " + total.value() + " results");
        }

        List<Hit<User>> hits = response.hits().hits();
        for (Hit<User> hit: hits) {
            User user = hit.source();
            assert user != null;
            log.info("Found userId " + user.getId() + ", name " + user.getName());
        }
    }

    /**
     * 多条件查询（IN ）
     *
     * @throws IOException ioexception
     */
    @Test
    void searchIn() throws IOException {
        /*方式一：terms多条件查询*/
        List<FieldValue> values = new ArrayList<>();
        values.add(FieldValue.of("zhaosi"));
        values.add(FieldValue.of("liuyifei"));

        Query queryIn = TermsQuery.of(t -> t.field("name.keyword").terms(new TermsQueryField.Builder()
                .value(values).build()))._toQuery();
        SearchResponse<User> response = elasticsearchClient.search(s -> s
                        // 我们要搜索的索引的名称
                        .index("users")
                        // 搜索请求的查询部分（搜索请求也可以有其他组件，如聚合）
                        .query(q -> q
                                .bool(b -> b
                                        .must(queryIn)
                                        .should(sh -> sh
                                                .match(t -> t.field("name")
                                                        .query("")))
                                )
                        ),
                // 匹配文档的目标类
                User.class
        );

        /*方式二，使用模板化搜索，直接编写 terms 语句
        * 官方文档：https://www.elastic.co/guide/en/elasticsearch/reference/8.3/search-template.html#search-template-convert-json
        * */
        /*elasticsearchClient.putScript(r -> r
                // 要创建的模板脚本的标识符
                .id("query-script")
                .script(s -> s
                        .lang("mustache")
                        .source("{\"query\":{\"terms\":{\"{{field}}\": {{#toJson}}values{{/toJson}} }}}")
                ));
        String field = "name.keyword";
        List<String> values = Arrays.asList("liuyifei","zhangya");
        String v = String.join(",",values);
        SearchTemplateResponse<User> response = elasticsearchClient.searchTemplate(r -> r
                        .index("users")
                        // 要使用的模板脚本的标识符
                        .id("query-script")
                        // 模板参数值
                        .params("field", JsonData.of(field))
                        .params("values", JsonData.of(values)),
                User.class
        );*/

        List<Hit<User>> hits = response.hits().hits();
        for (Hit<User> hit: hits) {
            User user = hit.source();
            assert user != null;
            log.info("Found userId " + user.getId() + ", name " + user.getName());
        }
    }

    /**
     * 嵌套搜索查询
     */
    @Test
    void searchTwo() throws IOException {
        String searchText = "liuyihu";
        int maxAge = 30;
        // byName、byMaxAge：分别为各个条件创建查询
        Query byName = MatchQuery.of(m -> m
                .field("name")
                .query(searchText)
        )
                //MatchQuery是一个查询变体，我们必须将其转换为 Query 联合类型
                ._toQuery();
        Query byMaxAge = RangeQuery.of(m -> m
                .field("age")
                // Elasticsearch 范围查询接受大范围的值类型。我们在这里创建最高价格的 JSON 表示。
                .gte(JsonData.of(maxAge))
        )._toQuery();
        SearchResponse<User> response = elasticsearchClient.search(s -> s
                        .index("users")
                        .query(q -> q
                                .bool(b -> b
                                        // 搜索查询是结合了文本搜索和最高价格查询的布尔查询
                                        .must(byName)
                                        .should(byMaxAge)
                                )
                        ),
                User.class
        );
        List<Hit<User>> hits = response.hits().hits();
        for (Hit<User> hit: hits) {
            User user = hit.source();
            assert user != null;
            log.info("Found userId " + user.getId() + ", name " + user.getName());
        }
    }

    /**
     * 模板化搜索
     * 模板化搜索是存储的搜索，可以使用不同的变量运行它。搜索模板让您无需修改应用程序代码即可更改搜索。
     * 在运行模板搜索之前，首先必须创建模板。这是一个返回搜索请求正文的存储脚本，通常定义为 Mustache 模板
     */
    @Test
    void templatedSearch() throws IOException {
        elasticsearchClient.putScript(r -> r
                // 要创建的模板脚本的标识符
                .id("query-script")
                .script(s -> s
                        .lang("mustache")
                        .source("{\"query\":{\"match\":{\"{{field}}\":\"{{value}}\"}}}")
                ));
        // 开始使用模板搜索
        String field = "name";
        String value = "liuyifei";
        SearchTemplateResponse<User> response = elasticsearchClient.searchTemplate(r -> r
                        .index("users")
                        // 要使用的模板脚本的标识符
                        .id("query-script")
                        // 模板参数值
                        .params("field", JsonData.of(field))
                        .params("value", JsonData.of(value)),
                User.class
        );

        List<Hit<User>> hits = response.hits().hits();
        for (Hit<User> hit: hits) {
            User user = hit.source();
            assert user != null;
            log.info("Found userId " + user.getId() + ", name " + user.getName());
        }
    }


    /**
     * 分页+排序条件搜索
     *
     * @throws IOException ioexception
     */
    @Test
    void paginationQuerySearch() throws IOException
    {
        int maxAge = 20;
        Query byMaxAge = RangeQuery.of(m -> m
                .field("age")
                .gte(JsonData.of(maxAge))
        )._toQuery();
        SearchResponse<User> response = elasticsearchClient.search(s -> s
                        .index("users")
                        .query(q -> q
                                .bool(b -> b
                                        .must(byMaxAge)
                                )
                        )
                        //分页查询，从第0页开始查询4个document
                        .from(0)
                        .size(4)
                         //按age降序排序
                        .sort(f -> f.field(o -> o.field("age")
                                .order(SortOrder.Desc))),
                User.class
        );
        List<Hit<User>> hits = response.hits().hits();
        List<User> userList = new ArrayList<>(hits.size());
        for (Hit<User> hit: hits) {
            User user = hit.source();
            userList.add(user);
        }
        log.info(JSONUtil.toJsonStr(userList));
    }

    /**
     * 分页+排序所有数据
     *
     * @throws IOException ioexception
     */
    @Test
    void paginationAllSearch() throws IOException
    {
        SearchResponse<User> response = elasticsearchClient.search(s -> s
                        .index("users")
                        .query(q -> q
                                .matchAll( m -> m)
                        )
                        .from(0)
                        .size(10)
                        .sort(f -> f.field(o -> o.field("age")
                                .order(SortOrder.Desc))),
                User.class
        );
        List<Hit<User>> hits = response.hits().hits();
        List<User> userList = new ArrayList<>(hits.size());
        for (Hit<User> hit: hits) {
            User user = hit.source();
            userList.add(user);
        }
        log.info(JSONUtil.toJsonStr(userList));
    }

    /**
     * 过滤字段
     *
     * @throws IOException ioexception
     */
    @Test
    void filterFieldSearch() throws IOException
    {
        SearchResponse<User> response = elasticsearchClient.search(s -> s
                        .index("users")
                        .query(q -> q
                                .matchAll( m -> m)
                        )
                        .sort(f -> f
                                .field(o -> o
                                        .field("age")
                                        .order(SortOrder.Desc)
                                )
                        )
                        .source(source -> source
                                .filter(f -> f
                                        .includes("name","id")
                                        .excludes(""))),
                User.class
        );
        List<Hit<User>> hits = response.hits().hits();
        List<User> userList = new ArrayList<>(hits.size());
        for (Hit<User> hit: hits) {
            User user = hit.source();
            userList.add(user);
        }
        log.info("过滤字段后：{}",JSONUtil.toJsonStr(userList));
    }

    /**
     * 模糊查询
     *
     * @throws IOException ioexception
     */
    @Test
    void fuzzyQuerySearch() throws IOException
    {
        SearchResponse<User> response = elasticsearchClient.search(s -> s
                        .index("users")
                        .query(q -> q
                                // 模糊查询
                                .fuzzy(f -> f
                                        // 需要判断的字段名称
                                        .field("name")
                                        // 需要模糊查询的关键词
                                        .value("liuyi")
                                        // fuzziness代表可以与关键词有误差的字数，可选值为0、1、2这三项
                                        .fuzziness("2")
                                )
                        )
                        .source(source -> source
                                .filter(f -> f
                                        .includes("name","id")
                                        .excludes(""))),
                User.class
        );
        List<Hit<User>> hits = response.hits().hits();
        List<User> userList = new ArrayList<>(hits.size());
        for (Hit<User> hit: hits) {
            User user = hit.source();
            userList.add(user);
        }
        log.info("模糊查询结果：{}",JSONUtil.toJsonStr(userList));
    }

    /**
     * 高亮查询
     *
     * @throws IOException ioexception
     */
    @Test
    void highlightQueryQuery() throws IOException
    {
        SearchResponse<User> response = elasticsearchClient.search(s -> s
                        .index("users")
                        .query(q -> q
                                .term(t -> t
                                        .field("name")
                                        .value("zhaosi"))
                        )
                        .highlight(h -> h
                                .fields("name", f -> f
                                        .preTags("<font color='red'>")
                                        .postTags("</font>")))
                        .source(source -> source
                                .filter(f -> f
                                        .includes("name","id")
                                        .excludes(""))),
                User.class
        );
        List<Hit<User>> hits = response.hits().hits();
        List<User> userList = new ArrayList<>(hits.size());
        for (Hit<User> hit: hits) {
            User user = hit.source();
            userList.add(user);
            for(Map.Entry<String, List<String>> entry : hit.highlight().entrySet())
            {
                System.out.println("Key = " + entry.getKey());
                entry.getValue().forEach(System.out::println);
            }
        }
        log.info(JSONUtil.toJsonStr(userList));
    }

    /**
     * 指定字段查询
     */
    @Test
    void specifyFieldQuery() throws IOException {
        int maxAge = 20;
        Query byMaxAge = RangeQuery.of(m -> m
                .field("age")
                .gte(JsonData.of(maxAge))
        )._toQuery();
        SearchResponse<User> response = elasticsearchClient
                .search(s -> s
                        .index("users")
                        .query(q -> q
                                .bool(b -> b
                                        .must(byMaxAge)
                                )
                        )
                        //分页查询，从第0页开始查询3个document
                        .from(0)
                        .size(4)
                        //按age降序排序
                        .sort(f -> f.field(o -> o.field("age")
                                .order(SortOrder.Desc))),
                User.class
        );
        List<Hit<User>> hits = response.hits().hits();
        List<User> userList = new ArrayList<>(hits.size());
        for (Hit<User> hit: hits) {
            User user = hit.source();
            userList.add(user);
        }
        log.info(JSONUtil.toJsonStr(userList));
    }
}
