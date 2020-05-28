package com.kevin.java.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

/**
 * @author kevin
 * @version 1.0
 * @description     作用到K,V格式的RDD，根据Key计数相同Key的数据集元素，返回一个Map<Integer, Object>
 * @createDate 2018/12/27
 */
public class CountByKey_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("CountByKey_RDD").setMaster("local");

        // 2.基于SparkConf创建JavaSparkContext，JavaSparkContext是Spark唯一通道
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.将list转为RDD
       JavaPairRDD<Integer, String> parallelizePairs = sc.parallelizePairs(Arrays.asList(
                new Tuple2<Integer,String>(1,"a"),
                new Tuple2<Integer,String>(2,"b"),
                new Tuple2<Integer,String>(3,"c"),
                new Tuple2<Integer,String>(4,"d"),
                new Tuple2<Integer,String>(4,"e")
        ));

        // 4.统计相同Key的Value次数，返回key
        Map<Integer, Object> countByKey = parallelizePairs.countByKey();
        for(Map.Entry<Integer,Object> entry : countByKey.entrySet()){
            System.out.println("key:"+entry.getKey()+"value:"+entry.getValue());
        }
        // 5.关闭
        sc.close();

    }
}
