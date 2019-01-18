package com.kevin.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * @author kevin
 * @version 1.0
 * @description 广播变量：
 *  * 1.不能将一个RDD使用广播变量广播出去，因为RDD是不存数据的，可以将RDD的结果广播出去。
 *  * 2.广播变量只能在Driver端定义，不能在Executor端定义。
 *  * 3.在Driver端可以修改广播变量的值，在Executor端不能修改广播变量的值。
 * @createDate 2019/1/2
 */
public class BroadCastDemo {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("BroadCastDemo").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> list = Arrays.asList("hello java");

        // 广播变量将list广播出去
        Broadcast<List<String>> broadcast = sc.broadcast(list);

        String file = "DTSparkCore\\src\\main\\resources\\records.txt";
        JavaRDD<String> lines = sc.textFile(file);

        lines.filter(new Function<String, Boolean>() {
            // 判断数据是否为广播数据，不是则过滤
            @Override
            public Boolean call(String s) throws Exception {
                return broadcast.value().contains(s);
            }
        }).foreach(new VoidFunction<String>() {
            // 遍历数据
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        // 关闭
        sc.close();

    }
}
