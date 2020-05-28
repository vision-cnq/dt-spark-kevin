package com.kevin.java.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @author kevin
 * @version 1.0
 * @description     map算子操作
 * map算子是输入一个元素输出一个元素
 * @createDate 2019/1/21
 */
public class Map {

    public static void main(String[] args) {
        // 1.创建sparkconf设置作业名称和模式，使用本地模式，最少需要两个线程，一个接收数据，一个处理数据
        SparkConf conf = new SparkConf().setAppName("Map").setMaster("local[2]");
        // 2.创建JavaStreamingContext有两种方式（SparkConf，SparkContext），设置等待时间为5秒
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // 3.设置日志级别
        jsc.sparkContext().setLogLevel("WARN");
        // 4.监控该目录下新增的文件，并获取数据
        String file = "DTSparkStreaming\\src\\main\\resources\\data\\";
        JavaDStream<String> lines = jsc.textFileStream(file);
        // 5.输入一条数据处理后输出一条
        JavaDStream<String> map = lines.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return s.trim();
            }
        });

        // 6.打印前100条结果数据
        map.print(100);
        // 7..启动程序
        jsc.start();
        // 8.等待被终止
        jsc.awaitTermination();
        // 9.关闭
        jsc.stop();

    }
}
