package com.kevin.java.window;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author kevin
 * @version 1.0
 * @description     可以获取到15秒之前到当前时间统计的数据
 *
 * 基于滑动窗口的热点搜索词实时统计
 * 当前设置窗口长度为15秒，滑动间隔为5秒
 * 每隔5秒，计算最近15秒内的数据，那么这个窗口大小就是15秒，有3个rdd，
 * 在没有计算之前，这些rdd是不会进行计算的，
 * 那么在计算的时候会将这3个rdd聚合起来，然后一起执行reduceByKeyAndWindow操作
 *
 * 优化的窗口计算： 比如，0~5秒+5~10秒+10~15秒，执行计算=结果1,
 * 下一次执行是结果1-0~5秒+15~20秒=结果2，下下次执行是结果2-5~10秒+20~25秒，依次类推...
 *
 * 未优化的窗口计算：比如，0~5秒+5~10秒+10~15秒，执行计算=结果1,
 * 下一次执行是5~10秒+10~15秒+15~20秒=结果2，下下次执行是10~15秒+15~20秒+20~25秒，依次类推...
 *
 * @createDate 2019/1/17
 */
public class ReduceByKeyAndWindow {

    public static void main(String[] args) {

        // 创建sparkconf设置作业名称和模式，使用本地模式，最少需要两个线程，一个接收数据，一个处理数据
        SparkConf conf = new SparkConf().setAppName("ReduceByKeyAndWindow").setMaster("local[3]");

        // 基于sparkconf创建JavaStreamingContext，设置接收数据间隔为5秒
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // 设置日志级别
        jsc.sparkContext().setLogLevel("WARN");
        String checkpointDirectory = "hdfs://Master:9000/sparkstreaming/checkpointWindow";

        // 没有优化的窗口函数可以不设置checkpoint目录
        // 优化的窗口函数必须设置checkpoint目录
        jsc.checkpoint(checkpointDirectory);
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("Master", 9999);

        // 设置窗口函数，每隔5秒，计算前面15秒的数据
        lines.window(Durations.seconds(15),Durations.seconds(5));

        // 获取数据并切分
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        // 将搜索词映射为<K,V>的tuple格式，得到所有的搜索词
        JavaPairDStream<String, Integer> wordPair = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        /**
         * 每隔5秒，计算最近15秒内的数据，那么这个窗口大小就是15秒，有3个rdd，
         * 在没有计算之前，这些rdd是不会进行计算的
         * 那么在计算的时候会将这3个rdd聚合起来，然后一起执行reduceByKeyAndWindow操作
         * reduceByKeyAndWindow是针对窗口操作的而不是针对DStream操作的
         */
        JavaPairDStream<String, Integer> wordCount = wordPair.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
    }, Durations.seconds(15), Durations.seconds(5));

        // window窗口操作优化，需要设置checkpoint
        // 设置的是5秒执行一次15秒内的数据，并不是像没优化的一样重新一个个批次相加
        // 而是用上次的结果减去最前面的一个批次以及加入最新的一个批次作为15秒的计算结果
        /*JavaPairDStream<String, Integer> wordCount = wordPair.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer - integer2;
            }
        }, Durations.seconds(15), Durations.seconds(5));*/

        // 打印一百条数据
        wordCount.print(100);

        // 启动sparkstreaming
        jsc.start();
        // 等待被终止
        jsc.awaitTermination();
        // 停止sparkstreaming
        jsc.close();

    }
}
