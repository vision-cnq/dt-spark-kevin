package com.kevin.java.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;

/**
 * @author kevin
 * @version 1.0
 * @description     根据聚合逻辑聚合数据集中的每个元素
 * @createDate 2018/12/27
 */
public class Reduce_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("Reduce_RDD").setMaster("local");

        // 2.基于SparkConf创建JavaSparkContext，JavaSparkContext是Spark的唯一通道
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.将list集合转为RDD
        JavaRDD<Integer> parallelize = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        // 4.使用reduce将其值累加
        Integer reduce = parallelize.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println(reduce);
        // 5.关闭JavaSparkContext
        sc.close();


    }
}
