package com.kevin.java.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @author kevin
 * @version 1.0
 * @description     返回一个新的DStream，它包含源DStream和otherDStream中元素的并集
 * @createDate 2019/1/21
 */
public class Union {

    public static void main(String[] args) {

        // 1.创建sparkconf设置作业名称和模式，使用本地模式，最少需要两个线程，一个接收数据，一个处理数据
        SparkConf conf = new SparkConf().setAppName("Union").setMaster("local[2]");
        // 2.创建JavaStreamingContext有两种方式（SparkConf，SparkContext），设置等待时间为5秒
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // 3.设置日志级别
        jsc.sparkContext().setLogLevel("WARN");
        // 4.监控该目录下新增的文件，并获取数据
        String file = "DTSparkStreaming\\src\\main\\resources\\data\\";

        JavaDStream<String> lines1 = jsc.textFileStream(file);
        JavaDStream<String> lines2 = jsc.textFileStream(file);
        // 5.取lines1和lines2中相同的元素
        JavaDStream<String> union = lines1.union(lines2);
        // 6.打印前1000条结果
        union.print(1000);
        // 7.启动程序
        jsc.start();
        // 8.等待被终止
        jsc.awaitTermination();
        // 9.关闭
        jsc.stop();
    }
}
