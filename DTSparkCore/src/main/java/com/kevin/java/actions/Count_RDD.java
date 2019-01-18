package com.kevin.java.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author kevin
 * @version 1.0
 * @description     返回结果集中的行数
 * @createDate 2018/12/27
 */
public class Count_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("Count_RDD").setMaster("local");

        // 2.JavaSparkContext是Spark的唯一通道
        JavaSparkContext sc = new JavaSparkContext(conf);

        String file = "DTSparkCore\\src\\main\\resources\\test.txt";

        // 4.获取文件数据
        JavaRDD<String> liens = sc.textFile(file);

        // 5.统计其行数
        long count = liens.count();

        System.out.println(count);

        // 6.关闭JavaSparkContext
        sc.close();
    }
}
