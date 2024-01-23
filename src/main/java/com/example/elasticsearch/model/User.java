package com.example.elasticsearch.model;

/**
 * @version 1.0.0
 * @className: User
 * @description:
 * @author: LiJunYi
 * @create: 2022/8/8 10:18
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown=true)
public class User
{
    @JsonProperty("id")
    private String id;

    @JsonProperty("name")
    private String name;

    @JsonProperty("age")
    private Integer age;

    @JsonProperty("sex")
    private String sex;

    public User()
    {

    }

    public User(String id, String name, Integer age, String sex) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.sex = sex;
    }

    @Override
    public String toString() {
        return "User{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", sex='" + sex + '\'' +
                '}';
    }
}
