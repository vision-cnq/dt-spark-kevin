package com.kevin.java;

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
 * @description     基于滑动窗口的热点搜索词实时统计
 *
 * 当前设置窗口长度为15秒，滑动间隔为5秒
 * 每隔5秒，计算最近15秒内的数据，那么这个窗口大小就是15秒，有3个rdd，
 * 在没有计算之前，这些rdd是不会进行计算的，
 * 那么在计算的时候会将这3个rdd聚合起来，然后一起执行reduceByKeyAndWindow操作
 *
 * @createDate 2019/1/17
 */
public class WindowOperator {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("WindowOperator").setMaster("local[3]");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
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

        // window窗口操作优化，计算两次每隔5秒计算前面15秒的数据
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

        wordCount.print(100);

        jsc.start();
        jsc.awaitTermination();
        jsc.close();

    }
}
