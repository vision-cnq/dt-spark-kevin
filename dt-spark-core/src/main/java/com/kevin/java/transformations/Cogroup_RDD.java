package com.kevin.java.transformations;

import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author kevin
 * @version 1.0
 * @description     当调用类型（K，V）和（K，W）的数据集时，
 * 返回一个数据集（K，（Iterable <V>，Iterable <W>））元组。此操作也被称做groupWith。
 * @createDate 2018/12/28
 */
public class Cogroup_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("Cogroup_RDD").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.创建student的数据
        List<Tuple2<String,String>> studentsList = Arrays.asList(
                new Tuple2<String,String>("1","zhangsan"),
                new Tuple2<String,String>("2","lisi"),
                new Tuple2<String,String>("2","wangwu"),
                new Tuple2<String,String>("3","maliu"));

        // 4.创建srore的数据
        List<Tuple2<String,String>> scoreList = Arrays.asList(
                new Tuple2<String,String>("1","100"),
                new Tuple2<String,String>("2","90"),
                new Tuple2<String,String>("3","80"),
                new Tuple2<String,String>("1","1000"),
                new Tuple2<String,String>("2","60"),
                new Tuple2<String,String>("3","50"));

        // 5.将student的数据转为RDD
        JavaPairRDD<String, String> studentRDD = sc.parallelizePairs(studentsList);

        // 6.将score的数据转为RDD
        JavaPairRDD<String, String> scoreRDD = sc.parallelizePairs(scoreList);

        // 7.cogroup是将所有相同的Key的value值聚合在一起，主动cogroup的是数组1，被动是数组2
        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> cogroup = studentRDD.cogroup(scoreRDD);

        // 8.遍历数据
        cogroup.foreach(new VoidFunction<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> s) throws Exception {
                System.out.println(s);
            }
        });

        // 9.关闭
        sc.close();

    }
}
