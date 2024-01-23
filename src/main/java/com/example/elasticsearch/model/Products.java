package com.example.elasticsearch.model;

import lombok.Data;

import java.util.List;

@Data
public class Products {

    /**
     * id
     */
    private String id;

    /**
     * 计数器
     */
    private Integer counter;

    /**
     * 标签
     */
    private List<String> tags;
}
