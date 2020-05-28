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
 * @description     在线实时单词统计
 * @createDate 2019/1/20
 */
public class WordCount {

    public static void main(String[] args) {

        // 1.创建sparkconf设置作业名称和模式，使用本地模式，最少需要两个线程，一个接收数据，一个处理数据
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[2]");
        // 2.基于sparkconf创建JavaStreamingContext，设置接收数据间隔为5秒
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // 3.使用socket数据源，设置节点和端口
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("Master", 9999);
        // 4.接收socket源的数据并切分返回不同的单词
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });
        // 5.初始化每个单词的次数为1
        JavaPairDStream<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        // 6.将相同的单词的次数相加
        JavaPairDStream<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        // 7.打印结果数据
        counts.print();
        // 8.启动sparkstreaming
        jsc.start();
        // 9.等待被停止
        jsc.awaitTermination();
        //JavaStreamingContext.stop()无参的stop方法会将sparkContext一同关闭，stop(false)
        jsc.stop(false);
        // 10.关闭
        jsc.close();

    }
}
