package com.kevin.java.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @author kevin
 * @version 1.0
 * @description     通过计算源DStream的每个RDD中的元素数量，返回单元素RDD的新DStream
 * @createDate 2019/1/20
 */
public class Count {
    public static void main(String[] args) {

        // 1.创建sparkconf设置作业名称和模式，使用本地模式，最少需要两个线程，一个接收数据，一个处理数据
        SparkConf conf = new SparkConf().setAppName("Count").setMaster("local[2]");
        // 2.创建JavaStreamingContext有两种方式（SparkConf，SparkContext），设置等待时间为5秒
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // 设置日志级别
        jsc.sparkContext().setLogLevel("WARN");
        String fileName = "DTSparkStreaming\\src\\main\\resources\\data";
        // 读取文件数据
        //JavaDStream<String> lines = jsc.textFileStream(fileName);
        // 3.读取socket源的数据
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("Master", 9999);
        // 4.统计数据
        JavaDStream<Long> count = lines.count();
        // 5.打印结果数据
        count.print();
        // 6.启动sparkstreaming
        jsc.start();
        // 7.等待被停止
        jsc.awaitTermination();
        // 8.关闭
        jsc.close();


    }
}
