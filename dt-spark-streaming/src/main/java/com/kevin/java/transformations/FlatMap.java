package com.kevin.java.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;

/**
 * @author kevin
 * @version 1.0
 * @description 与map相似，但每个输入项可以映射到0个或更多的输出项
 * 也就是说输入项为1，输出项可以为0到多个
 * @createDate 2019/1/21
 */
public class FlatMap {

    public static void main(String[] args) {

        // 1.创建sparkconf设置作业名称和模式，使用本地模式，最少需要两个线程，一个接收数据，一个处理数据
        SparkConf conf = new SparkConf().setAppName("FlatMap").setMaster("local[2]");
        // 2.创建JavaStreamingContext有两种方式（SparkConf，SparkContext），设置等待时间为5秒
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // 3.设置日志级别
        jsc.sparkContext().setLogLevel("WARN");
        // 4.监控该目录下新增的文件，并获取数据
        String file = "DTSparkStreaming\\src\\main\\resources\\data\\";
        JavaDStream<String> lines = jsc.textFileStream(file);

        // 5.读入的每一行，然后每一行中按空格切分，将每个单词以Iterable的形式返回。即：输入一行，输出多个单词
        JavaDStream<String> flatMap = lines.flatMap(new FlatMapFunction<String, String>() {

            public Iterable<String> call(String t) throws Exception {
                return Arrays.asList(t.split(" "));
            }
        });
        // 6.打印前100
        flatMap.print(100);
        // 7.启动
        jsc.start();
        // 8.等待被停止
        jsc.awaitTermination();
        // 9.关闭
        jsc.close();
    }
}
