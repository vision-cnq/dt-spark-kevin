package com.kevin.java.window;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author kevin
 * @version 1.0
 * @description     统计key在滑动窗口中出现的次数
 *
 * countByValueAndWindow(windowLength,slideInterval, [numTasks]):
 *
 * （K，V）格式的Dstream使用时，每个Key的Value值根据Key在滑动窗口出现的次数，返回（K，Long）格式的Dstream。
 * @createDate 2019/1/21
 */
public class CountByValueAndWindow {

    public static void main(String[] args) {

        String file = "DTSparkStreaming\\src\\main\\resources\\";

        // 创建sparkconf设置作业名称和模式，使用本地模式，最少需要两个线程，一个接收数据，一个处理数据
        SparkConf conf = new SparkConf().setAppName("ReduceByWindow").setMaster("local[2]");
        // 创建JavaStreamingContext有两种方式（SparkConf，SparkContext），设置等待时间为5秒
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // 设置日志级别
        jsc.sparkContext().setLogLevel("WARN");
        // 设置checkpoint
        jsc.checkpoint(file+"checkpoint");
        // 监控该目录下新增的文件，并获取数据
        JavaDStream<String> lines = jsc.textFileStream(file+"data\\");

        // 切分数据返回多条数据
        JavaDStream<String> flatMap = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });
        // 每个次数初始化为1
        JavaPairDStream<String, Integer> mapToPair = flatMap.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s.trim(), 1);
            }
        });
        // 获取在滑动窗口间每个key出现的次数
        JavaPairDStream<Tuple2<String, Integer>, Long> window = mapToPair.countByValueAndWindow(Durations.seconds(15), Durations.seconds(5));
        // 打印结果数据
        window.print();
        // 启动
        jsc.start();
        // 等待被停止
        jsc.awaitTermination();
        // 关闭
        jsc.close();
    }
}
