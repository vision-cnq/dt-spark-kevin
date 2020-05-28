package com.kevin.java.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

/**
 * @author kevin
 * @version 1.0
 * @description     take是获取指定的数据
 * @createDate 2019/1/2
 */
public class Take_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("Take_RDD").setMaster("local");

        // 2.JavaSparkContext是Spark的唯一通道
        JavaSparkContext sc = new JavaSparkContext(conf);

        String file = "DTSparkCore\\src\\main\\resources\\test.txt";

        // 4.获取文件数据
        JavaRDD<String> liens = sc.textFile(file);

        // 5.获取rdd中指定的前几个数据，如获取rdd中前两个数据
        List<String> take = liens.take(2);

        // 6.遍历数据
        for (String s:take) {
            System.out.println(s);
        }

        // 7.关闭JavaSparkContext
        sc.close();
    }
}
