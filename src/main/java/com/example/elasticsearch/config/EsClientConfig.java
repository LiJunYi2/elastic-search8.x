package com.example.elasticsearch.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

/**
 * es集群客户端构建方式
 * @author raxcl
 * @date 2023-07-05 16:56:39
 */
@Configuration
public class EsClientConfig {

    /**
     * elasticsearch:
     *   # 多个IP逗号隔开
     *   hosts: 10.4.172.72:9200,10.7.136.73:9200,10.2.146.74:9200
     *   username: elastic
     *   password: Mvase435@al
     */


    /**
     * 多个IP逗号隔开
     */
    @Value("${elasticsearch.hosts}")
    private String hosts;
    @Value("${elasticsearch.username}")
    private String username;
    @Value("${elasticsearch.password}")
    private String password;

    /**
     * 同步方式
     *
     * @return ElasticsearchClient
     */
    @Bean
    public ElasticsearchClient elasticsearchClient() {
        return new ElasticsearchClient(clientInit());
    }

    /**
     * 异步方式
     *
     * @return ElasticsearchClient
     */
//    @Bean
//    public ElasticsearchAsyncClient elasticsearchAsyncClient() {
//        return new ElasticsearchAsyncClient(clientInit());
//    }

    private ElasticsearchTransport clientInit() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        // Create the low-level client
        HttpHost[] httpHosts = toHttpHost();
        RestClient restClient = RestClient.builder(httpHosts)
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider))
                .build();
        // Create the transport with a Jackson mapper
        return new RestClientTransport(restClient, new JacksonJsonpMapper());
    }

    /**
     * 解析配置的字符串hosts，转为HttpHost对象数组
     *
     * @return HttpHost[]
     */
    private HttpHost[] toHttpHost() {
        if (!StringUtils.hasLength(hosts)) {
            throw new RuntimeException("invalid elasticsearch configuration. elasticsearch.hosts不能为空！");
        }

        // 多个IP逗号隔开
        String[] hostArray = hosts.split(",");
        HttpHost[] httpHosts = new HttpHost[hostArray.length];
        HttpHost httpHost;
        for (int i = 0; i < hostArray.length; i++) {
            String[] strings = hostArray[i].split(":");
            httpHost = new HttpHost(strings[0], Integer.parseInt(strings[1]), "http");
            httpHosts[i] = httpHost;
        }

        return httpHosts;
    }
}
