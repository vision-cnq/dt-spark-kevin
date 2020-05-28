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
 * @description     ZipWithIndex会将RDD中的元素和这个元素在RDD中的索引好（从0开始），组合成K,V对
 * @createDate 2018/12/31
 */
public class ZipWithIndex_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("ZipWithIndex_RDD").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.创建list数据并转为RDD
        JavaRDD<String> nameRDD = sc.parallelize(Arrays.asList("java", "scala", "python"));

        JavaPairRDD<String, Long> zipWithIndex = nameRDD.zipWithIndex();

        zipWithIndex.foreach(new VoidFunction<Tuple2<String, Long>>() {
            @Override
            public void call(Tuple2<String, Long> s) throws Exception {
                System.out.println(s);
            }
        });

        // 关闭
        sc.close();

    }
}
