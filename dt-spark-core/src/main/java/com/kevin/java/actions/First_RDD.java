package com.kevin.java.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @author kevin
 * @version 1.0
 * @description     first是获取第一个数据
 * @createDate 2018/12/27
 */
public class First_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("First_RDD").setMaster("local");

        // 2.基于SparkConf创建JavaSparkContext，JavaSparkContext是Spark唯一通道
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.将list转为RDD
        JavaRDD<String> parallelize = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));

        // 4.获取rdd的第一个数据
        String first = parallelize.first();

        System.out.println("first: " + first);

        // 5.关闭
        sc.close();
    }
}
