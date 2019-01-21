package com.kevin.java.window;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;
/**
 * @author kevin
 * @version 1.0
 * @description     window:返回基于源DStream的窗口批次计算的新DStream
 * @createDate 2019/1/21
 */
public class Window {

    public static void main(String[] args) {

        // 1.创建sparkconf设置作业名称和模式，使用本地模式，最少需要两个线程，一个接收数据，一个处理数据
        SparkConf conf = new SparkConf().setAppName("Window").setMaster("local[2]");
        // 2.创建JavaStreamingContext有两种方式（SparkConf，SparkContext），设置等待时间为5秒
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // 3.设置日志级别
        jsc.sparkContext().setLogLevel("WARN");
        // 4.监控该目录下新增的文件，并获取数据
        String file = "DTSparkStreaming\\src\\main\\resources\\data\\";
        JavaDStream<String> lines = jsc.textFileStream(file);
        // 方式一：设置窗口函数
        /*JavaDStream<String> window2 = lines.window(Durations.seconds(15), Durations.seconds(5));

        // 遍历输出
		window2.foreachRDD(new VoidFunction<JavaRDD<String>>() {

			public void call(JavaRDD<String> arg0) throws Exception {
				System.out.println("***************");
			}
		});*/

        // 方式二：首先将textFileStream转换为tuple格式统计word字数
		JavaPairDStream<String, Integer> mapToPair = lines.flatMap(new FlatMapFunction<String, String>() {

			public Iterable<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" "));
			}
		}).mapToPair(new PairFunction<String, String, Integer>() {

			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t.trim(), 1);
			}
		});
        // 然后设置窗口函数
		JavaPairDStream<String, Integer> window = mapToPair.window(Durations.seconds(15),Durations.seconds(5));
		// 打印结果数据
		window.count().print();

        // 5.启动
        jsc.start();
        // 6.等待被停止
        jsc.awaitTermination();
        // 7.关闭
        jsc.close();
    }
}
