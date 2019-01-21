package com.kevin.java.window;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * @author kevin
 * @version 1.0
 * @description     reduceByWindow(func, windowLength, slideInterval):
 *
 * 通过使用func在滑动间隔内通过流中的元素聚合创建返回一个新的单元素流。该函数是相关联的，以便它可以并行计算。
 *
 * 窗口长度（windowLength）：窗口的持续时间
 * 滑动间隔（slideInterval）：执行窗口操作的间隔
 * @createDate 2019/1/21
 */
public class ReduceByWindow {

    public static void main(String[] args) {

        // 创建sparkconf设置作业名称和模式，使用本地模式，最少需要两个线程，一个接收数据，一个处理数据
        SparkConf conf = new SparkConf().setAppName("ReduceByWindow").setMaster("local[2]");
        // 创建JavaStreamingContext有两种方式（SparkConf，SparkContext），设置等待时间为5秒
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // 设置日志级别
        jsc.sparkContext().setLogLevel("WARN");
        // 监控该目录下新增的文件，并获取数据
        String file = "DTSparkStreaming\\src\\main\\resources\\data\\";
        JavaDStream<String> textFileStream = jsc.textFileStream(file);

        // 首先将textFileStream转换为tuple格式统计word字数
        JavaPairDStream<String, Integer> mapToPair = textFileStream.flatMap(new FlatMapFunction<String, String>() {

            public Iterable<String> call(String t) throws Exception {
                return Arrays.asList(t.split(" "));
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {

            public Tuple2<String, Integer> call(String t) throws Exception {
                return new Tuple2<String, Integer>(t.trim(), 1);
            }
        });

        // 将两个元祖的key都拼接在一起，并将两个元祖的次数相加
        JavaDStream<Tuple2<String, Integer>> reduceByWindow = mapToPair.reduceByWindow(new Function2<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            public Tuple2<String, Integer> call(Tuple2<String, Integer> v1,Tuple2<String, Integer> v2) throws Exception {
                return new Tuple2<String, Integer>(v1._1+"**"+v2._1, v1._2+v2._2);
            }
            // 设置每5秒执行一次最近15秒内rdd
        }, Durations.seconds(15), Durations.seconds(5));

        // 打印结果
        reduceByWindow.print();
        // 启动
        jsc.start();
        // 等待被停止
        jsc.awaitTermination();
        // 关闭
        jsc.close();
    }

}
