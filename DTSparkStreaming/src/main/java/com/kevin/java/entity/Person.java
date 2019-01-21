package com.kevin.java.entity;

import java.io.Serializable;

/**
 * @author kevin
 * @version 1.0
 * @description     自定义类要实现序列化接口
 * 自定义类访问级别必须是public
 * RDD转成DataFrame会把自定义类中字段的名称按assci排序
 * @createDate 2019/1/6
 */
public class Person implements Serializable {

    private Integer id ;
    private String name;
    private String gender;
    private Integer age;

    public Person() {
    }

    public Person(Integer id, String name, String gender, Integer age) {
        this.id = id;
        this.name = name;
        this.gender = gender;
        this.age = age;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Person{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", gender='" + gender + '\'' +
                ", age=" + age +
                '}';
    }
}
