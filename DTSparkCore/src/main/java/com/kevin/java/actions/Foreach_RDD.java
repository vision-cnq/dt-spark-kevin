package com.kevin.java.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

/**
 * @author kevin
 * @version 1.0
 * @description     使用spark的foreach函数进行遍历数据
 * @createDate 2018/12/27
 */
public class Foreach_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("Foreach_RDD").setMaster("local");

        // 2.基于SparkConf创建JavaSparkContext，JavaSparkContext是Spark唯一通道
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.将List集合转为RDD
        JavaRDD<Integer> parallelize = sc.parallelize(Arrays.asList(1,2,3,4,5));

        // 4.使用Spark函数的foreach遍历rdd数据集
        parallelize.foreach(new VoidFunction<Integer>(){
            @Override
            public void call(Integer s) throws Exception {
                System.out.println(s);
            }
        });
        // 5.关闭SparkContext
        sc.close();
    }
}
