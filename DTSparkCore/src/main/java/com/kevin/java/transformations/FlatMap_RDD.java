package com.kevin.java.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

/**
 * @author kevin
 * @version 1.0
 * @description     flatMap输入一条数据，输出多条数据。一对多：类型是list对象
 * @createDate 2018/12/27
 */
public class FlatMap_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("FlatMap_RDD").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        String file = "DTSparkCore\\src\\main\\resources\\test2.txt";
        // 3.读取文件数据
        JavaRDD<String> lines = sc.textFile(file);

        // 4.使用flatMap切分数据
        JavaRDD<String> flatMap = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        // 5.使用foreach遍历
        flatMap.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println("foreach: " + s);
            }
        });

        // 6.关闭
        sc.close();
   }
}
