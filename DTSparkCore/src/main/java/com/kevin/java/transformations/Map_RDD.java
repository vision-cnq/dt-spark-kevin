package com.kevin.java.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

/**
 * @author kevin
 * @version 1.0
 * @description     map处理传入的每个元素，输入一条，输出一条。一对一：类型是对象
 * @createDate 2018/12/27
 */
public class Map_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("Map_RDD").setMaster("local");

        // 2.基于SparkConf创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.创建数据
        List<String> list = Arrays.asList("Hello World", "Word Count");

        // 4.将集合转为rdd
        JavaRDD<String> linesRDD = sc.parallelize(list);

        // 5.使用map切分数据，map返回一个结果
        JavaRDD<Object> mapRDD = linesRDD.map(new Function<String, Object>() {
            @Override
            public Object call(String s) throws Exception {
                return s.split(" ");
            }
        });

        // 6.将mapRDD转为list集合,并遍历输出
        List<Object> collect1 = mapRDD.collect();
        for (Object obj:collect1) {
            System.out.println("collect1: " + obj);
        }

        // 7.关闭
        sc.close();

    }
}
