package com.example.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@SpringBootTest
@Slf4j
class ElasticsearchApplicationTests
{


    /**
     * elasticsearch客户机构建
     *
     * @throws IOException ioexception
     */
    @Test
    void ElasticsearchClientBuild() throws IOException {
        // Create the low-level client
        RestClient restClient = RestClient.builder(
                new HttpHost("127.0.0.1", 9200)).build();

        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        // And create the API client
        ElasticsearchClient elasticsearchClient  = new ElasticsearchClient(transport);
        System.out.println("elasticsearchClient = " + elasticsearchClient);
        elasticsearchClient.shutdown();
        restClient.close();
    }
}
