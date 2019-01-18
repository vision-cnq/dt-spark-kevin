package com.kevin.java.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author kevin
 * @version 1.0
 * @description     将每个分区里面的元素进行聚合，然后用combine函数将每个分区的结果和初始值(zeroValue)进行combine操作。
 * 这个函数最终返回的类型不需要和RDD中元素类型一致。
 * @createDate 2019/1/2
 */
public class Aggregate_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("Aggregate_RDD").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.创建list数据
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(9, 5, 7, 4, 4, 2, 2),3);

        // 4.设置中立值作为计算，先对分区的数据聚合，再将每个分区的结果和初始值进行comb
        Integer aggregateRDD = rdd.aggregate(2, new Function2<Integer, Integer, Integer>() {
            // 对同个partition的值进行合并
            // 从设置的中立值开始加第一个值
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                System.out.println("Sequ: "+ v1 +"\t"+ v2);
                return v1 + v2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            // 将每个partition的结果值进行合并
            // 从设置的中立值开始加第一个结果值
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                System.out.println("Comb: "+v1 +"\t"+ v2);
                return v1 + v2;
            }
        });

        System.out.println("aggregate处理的最终结果: " + aggregateRDD);

        // 5.关闭
        sc.close();
    }
}
