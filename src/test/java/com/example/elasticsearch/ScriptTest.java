package com.example.elasticsearch;

import cn.hutool.json.JSONUtil;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ScriptLanguage;
import co.elastic.clients.elasticsearch._types.ScriptSortType;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import co.elastic.clients.json.JsonData;
import com.example.elasticsearch.model.Products;
import com.example.elasticsearch.model.User;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.util.Maps;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import java.io.IOException;
import java.util.*;

/**
 * Script的简单使用
 * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-scripting-using.html">官方文档</a>
 * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/8.6/search-fields.html#script-fields">官方文档</a>
 * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/scripts-and-search-speed.html">官方文档</a>
 * @author LiJY
 * @date 2024/01/23
 */
@SpringBootTest
@Slf4j
public class ScriptTest {

    @Autowired
    private ElasticsearchClient elasticsearchClient;

    /**
     * 添加示例商品索引
     *
     * @throws IOException ioexception
     */
    @Test
    void addOneDocument () throws IOException
    {
        // 1、using the fluent DSL
        Products products = new Products();
        products.setId("5");
        products.setCounter(240);
        products.setTags(Collections.singletonList("yellow"));
        IndexResponse indexResponse = elasticsearchClient.index(s ->
                // 索引
                s.index("products")
                        // ID
                        .id(products.getId())
                        // 文档
                        .document(products)
        );
        log.info("result:{}",indexResponse.result().jsonValue());
    }

    /**
     * 脚本修改商品 counter 属性
     */
    @Test
    public void updateProductsCounter(){
        String indexName = "products";
        int fromCounter = 1;
        Integer newCounter = 666;
        Map<String, JsonData> map = Maps.newHashMap("newCounter", JsonData.of(newCounter));
        try {
            elasticsearchClient.updateByQuery(d -> d
                    .index(indexName)
                    .query(q -> q
                            .term(t -> t
                                    .field("counter")
                                    .value(fromCounter)
                            ))
                    .script(s -> s
                            .inline(src -> src
                                    .lang("painless")
                                    .source("ctx._source.counter += params.newCounter")
                                    .params(map)
                            )
                    )
            );
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    /**
     *  脚本修改商品 tag 属性
     */
    @Test
    public void updateProductsTags(){
        String indexName = "products";
        String fromTag = "red";
        String addTag = "yellow";
        Map<String, JsonData> map = Maps.newHashMap("addTag", JsonData.of(addTag));
        try {
            elasticsearchClient.updateByQuery(d -> d
                    .index(indexName)
                    .query(q -> q
                            .term(t -> t
                                    .field("tags")
                                    .value(fromTag)
                            ))
                    .script(s -> s
                            .inline(src -> src
                                    .lang("painless")
                                    .source("ctx._source.tags.add(params.addTag)")
                                    .params(map)
                            )
                    )
            );
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    @Test
    public void removeProductTags(){
        String indexName = "products";
        // String fromTag = "red,blue";
        String deleteTag = "blue";
        Map<String, JsonData> map = Maps.newHashMap("deleteTag", JsonData.of(deleteTag));
        try {
            elasticsearchClient.updateByQuery(d -> d
                    .index(indexName)
//                    .query(q -> q
//                            .term(t -> t
//                                    .field("tags")
//                                    .value(fromTag)
//                            ))
                    .script(s -> s
                            .inline(src -> src
                                    .lang("painless")
                                    .source("if (ctx._source.tags.contains(params.deleteTag)) { ctx._source.tags.remove(ctx._source.tags.indexOf(params.deleteTag)) }")
                                    .params(map)
                            )
                    )
            );
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    /**
     * 搜索全部订单并排序
     * 通过脚本进行排序，指定某个值始终在第一个，其余的数据按照别的字段排序
     * @throws IOException ioexception
     */
    @Test
    void searchAllToOrder() throws IOException {
        int searchText = 667;
        SearchResponse<Products> response = elasticsearchClient.search(s -> s
                        // 我们要搜索的索引的名称
                        .index("products")
                        // 搜索请求的查询部分（搜索请求也可以有其他组件，如聚合）
                        .query(q -> q.matchAll(matchAll -> matchAll))
                        .size(100)
                        .sort(sort ->
                                sort.script(sortScript ->
                                        sortScript.type(ScriptSortType.Number)
                                                .order(SortOrder.Desc)
                                                .script(script ->
                                                        script.inline(inline ->
                                                                inline.source("if(params['_source']['counter'] == params.counter){\n" +
                                                                        "                        1\n" +
                                                                        "                      } else {\n" +
                                                                        "                        0\n" +
                                                                        "                      }")
                                                                        .params("counter",JsonData.of(searchText))
                                                        )
                                                )
                                )
                        )
                        .sort(sort -> sort.field(filed ->
                                filed.field("counter").order(SortOrder.Asc))
                        ),
                // 匹配文档的目标类
                Products.class
        );
        TotalHits total = response.hits().total();
        boolean isExactResult = total.relation() == TotalHitsRelation.Eq;

        if (isExactResult) {
            log.info("There are " + total.value() + " results");
        } else {
            log.info("There are more than " + total.value() + " results");
        }

        List<Hit<Products>> hits = response.hits().hits();
        for (Hit<Products> hit: hits) {
            Products products = hit.source();
            assert products != null;
            log.info("Found id " + products.getId() + ", counter " + products.getCounter());
        }
    }

    /**
     * 转换字典值简单示例
     *
     * @throws IOException ioexception
     */
    @Test
    void convertDictionaryValues() throws IOException {
        SearchRequest request = SearchRequest.of(searchRequest ->
                searchRequest.index("users")
                        .query(query -> query.matchAll(matchAll -> matchAll))
                        // 不加这句，则 _source 不会返回，值返回 fields
                        .source(config -> config.filter(filter -> filter.includes("*")))
                        .scriptFields("age_format",field ->
                                field.script(script ->
                                        script.inline(inline ->
                                                inline.lang(ScriptLanguage.Painless)
                                                        .source(" // 判断 age 字段是否存在\n" +
                                                                "          if(doc['age'].size() == 0){\n" +
                                                                "            return \"--\";\n" +
                                                                "          }\n" +
                                                                "        \n" +
                                                                "          if(doc['age'].value < 20){\n" +
                                                                "            return \"青年\";\n" +
                                                                "          }else if(doc['age'].value < 40){\n" +
                                                                "            return \"中年\";\n" +
                                                                "          }else if(doc['age'].value == ''){\n" +
                                                                "            return \"未知\";\n" +
                                                                "          }else{\n" +
                                                                "            return \"**\";\n" +
                                                                "          }")
                                        )
                                )
                        )
                        .size(100)
        );
        SearchResponse<User> response = elasticsearchClient.search(request, User.class);
        List<Hit<User>> hits = response.hits().hits();
        List<User> userList = new ArrayList<>(hits.size());
        for (Hit<User> hit: hits) {
            User user = hit.source();
            userList.add(user);
            log.info("user {}: age_format:{}",user.getName(),hit.fields().get("age_format"));
        }
        log.info(JSONUtil.toJsonStr(userList));
    }
}
