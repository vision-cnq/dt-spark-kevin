package com.kevin.java.output;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @author kevin
 * @version 1.0
 * @description     在运行streaming程序的Driver节点端打印DStream每批次数据的前十个元素，常用于开发和调试
 * @createDate 2019/1/21
 */
public class Print {

    public static void main(String[] args) {

        // 1.创建sparkconf设置作业名称和模式，使用本地模式，最少需要两个线程，一个接收数据，一个处理数据
        SparkConf conf = new SparkConf().setAppName("Print").setMaster("local[2]");
        // 2.创建JavaStreamingContext有两种方式（SparkConf，SparkContext），设置等待时间为5秒
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // 3.设置日志级别
        jsc.sparkContext().setLogLevel("WARN");
        // 4.监控该目录下新增的文件，并获取数据
        String file = "DTSparkStreaming\\src\\main\\resources\\data\\";
        JavaDStream<String> lines = jsc.textFileStream(file);
        // 5.打印结果数据
        lines.print(100);
        // 6.启动程序
        jsc.start();
        // 7.等待被终止
        jsc.awaitTermination();
        // 8.关闭
        jsc.stop();
    }
}
