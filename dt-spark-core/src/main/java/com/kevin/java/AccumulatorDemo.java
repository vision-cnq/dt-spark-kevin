package com.kevin.java;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * @author kevin
 * @version 1.0
 * @description     累加器在Driver端定义赋初始值和读取，在Executor端累加。
 * @createDate 2019/1/2
 */
public class AccumulatorDemo {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("AccumulatorDemo").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Accumulator<Integer> accumulator = sc.accumulator(0);

        String file = "DTSparkCore\\src\\main\\resources\\records.txt";
        JavaRDD<String> rdd = sc.textFile(file);

        rdd.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                accumulator.add(1);
            }
        });

        System.out.println(accumulator.value());

        sc.close();
    }
}
