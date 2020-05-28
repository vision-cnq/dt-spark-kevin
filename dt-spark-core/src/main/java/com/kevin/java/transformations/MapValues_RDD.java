package com.kevin.java.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author kevin
 * @version 1.0
 * @description     mapValues是对<K,V>的V值进行map操作
 * @createDate 2018/12/28
 */
public class MapValues_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("MapValues_RDD").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Integer> parallelizePairs = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String,Integer>("a",100),
                new Tuple2<String,Integer>("b",200),
                new Tuple2<String,Integer>("c",300)
        ));

        JavaPairRDD<String, String> mapValues = parallelizePairs.mapValues(new Function<Integer, String>() {
            @Override
            public String call(Integer integer) throws Exception {
                return integer + "~";
            }
        });

        mapValues.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> s) throws Exception {
                System.out.println(s);
            }
        });

        // 关闭
        sc.close();
    }
}
