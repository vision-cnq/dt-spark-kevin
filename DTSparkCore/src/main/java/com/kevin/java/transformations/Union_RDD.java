package com.kevin.java.transformations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

/**
 * @author kevin
 * @version 1.0
 * @description     合并两个RDD，不去重，要求两个RDD钟的元素类型一致，逻辑上合并
 * @createDate 2018/12/27
 */
public class Union_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("Union_RDD").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.创建list数据
        List<String> data1 = Arrays.asList("KEVIN", "COCO");
        List<String> data2 = Arrays.asList("boy", "grid");

        // 4.将list转为rdd
        JavaRDD<String> data1RDD = sc.parallelize(data1);
        JavaRDD<String> data2RDD = sc.parallelize(data2);

        // 5.合并两个RDD，不去重，要求两个RDD中的元素类型一致
        JavaRDD<String> union = data1RDD.union(data2RDD);

        // 6.遍历
        union.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        // 7.关闭
        sc.close();
    }
}
