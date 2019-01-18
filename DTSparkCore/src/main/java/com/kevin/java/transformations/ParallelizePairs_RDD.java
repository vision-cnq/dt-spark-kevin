package com.kevin.java.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author kevin
 * @version 1.0
 * @description     ParallelizePairs将Tuple2<K,V>类型的list转为成RDD
 * @createDate 2018/12/28
 */
public class ParallelizePairs_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("ParallelizePairs_RDD").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.将Tuple2类型的list转为成RDD
        JavaPairRDD<Integer, String> parallelizePairs = sc.parallelizePairs(Arrays.asList(
                new Tuple2<Integer, String>(0, "aa"),
                new Tuple2<Integer, String>(1, "a"),
                new Tuple2<Integer, String>(2, "b"),
                new Tuple2<Integer, String>(3, "c")));

        // 4.遍历数据
        parallelizePairs.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> s) throws Exception {
                System.out.println(s);
            }
        });

        // 5.关闭
        sc.close();
    }
}
