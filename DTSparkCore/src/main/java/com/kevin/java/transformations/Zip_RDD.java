package com.kevin.java.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author kevin
 * @version 1.0
 * @description     zip将两个非KV格式的RDD，通过一一对应的关系压缩成KV格式的RDD
 * 要求：分区数和分区中的元素个数相等
 * @createDate 2018/12/31
 */
public class Zip_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("Zip_RDD").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.创建list数据并转为RDD
        JavaRDD<String> nameRDD = sc.parallelize(Arrays.asList("java", "scala", "python"));
        JavaRDD<Integer> scoreRDD = sc.parallelize(Arrays.asList(100,200,300));

        // 4.将值连接构成元祖，必须两个rdd的元素数量，partition数量一致
        JavaPairRDD<String, Integer> zip = nameRDD.zip(scoreRDD);

        // 5.遍历
        zip.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> s) throws Exception {
                System.out.println(s);
            }
        });

        // 关闭
        sc.close();
    }
}
