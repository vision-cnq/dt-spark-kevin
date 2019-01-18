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
 * @description     相同的Key分组并将其Value值聚合在一起
 * @createDate 2018/12/28
 */
public class GroupByKey_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("GroupByKey_RDD").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.将Tuple2的数据转为rdd
        JavaPairRDD<String, Integer> parallelizePairs = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String,Integer>("a",100),
                new Tuple2<String,Integer>("b",200),
                new Tuple2<String,Integer>("b",100),
                new Tuple2<String,Integer>("c",200),
                new Tuple2<String,Integer>("d",100),
                new Tuple2<String,Integer>("d",300)
        ));

        // 4.将相同的Key分组并将其Value值聚合在一起
        JavaPairRDD<String, Iterable<Integer>> groupByKey = parallelizePairs.groupByKey();

        // 5.遍历数据
        groupByKey.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>(){
            @Override
            public void call(Tuple2<String, Iterable<Integer>> s) throws Exception {
                System.out.println(s);
            }
        });

        // 6.关闭
        sc.close();

    }
}
