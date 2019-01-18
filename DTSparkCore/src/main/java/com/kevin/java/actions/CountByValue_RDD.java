package com.kevin.java.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author kevin
 * @version 1.0
 * @description     根据数据集每个元素相同的内容来计数，返回相同内容元素对应的条数
 * @createDate 2018/12/27
 */
public class CountByValue_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("CountByValue_RDD").setMaster("local");
        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer,String>> list = new ArrayList();
        list.add(new Tuple2<Integer,String>(1,"a"));
        list.add(new Tuple2<Integer,String>(2,"b"));
        list.add(new Tuple2<Integer,String>(2,"c"));
        list.add(new Tuple2<Integer,String>(3,"c"));
        list.add(new Tuple2<Integer,String>(4,"d"));
        list.add(new Tuple2<Integer,String>(4,"d"));

        // 3.将list数据转为RDD
        JavaPairRDD<Integer, String> parallelizePairs = sc.parallelizePairs(list);

        // 4.统计相同key的value次数，返回(key,value)
        Map<Tuple2<Integer, String>, Long> countByValue = parallelizePairs.countByValue();

        // 5.遍历统计好的结果集
        for (Map.Entry<Tuple2<Integer,String>, Long> entry : countByValue.entrySet()) {
            System.out.println(entry.getKey()+" : "+entry.getValue());
        }

        // 6.关闭
        sc.close();
    }
}
