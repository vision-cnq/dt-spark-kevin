package com.kevin.java.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

/**
 * @author kevin
 * @version 1.0
 * @description     取一个RDD与另一个RDD的差集
 * @createDate 2018/12/30
 */
public class Subtract_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("Subtract_RDD").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.分别创建两个相同类型的list转换成rdd
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a","b","c"));
        JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("a","e","f"));

        // 4.取一个rdd与另一个rdd的差集
        JavaRDD<String> subtract = rdd1.subtract(rdd2);

        // 5.遍历
        subtract.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        // 6.关闭
        sc.close();
    }
}
