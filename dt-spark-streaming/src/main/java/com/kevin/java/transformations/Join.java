package com.kevin.java.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author kevin
 * @version 1.0
 * @description     当（k,v）格式和（k,w）格式的两个DStream使用时，返回一个新的（k,（v,w））格式的DStream
 * 注意，join作用在（k,v）格式的DStream
 * @createDate 2019/1/21
 */
public class Join {

    public static void main(String[] args) {

        // 1.创建sparkconf设置作业名称和模式，使用本地模式，最少需要两个线程，一个接收数据，一个处理数据
        SparkConf conf = new SparkConf().setAppName("Join").setMaster("local[2]");
        // 2.创建JavaStreamingContext有两种方式（SparkConf，SparkContext），设置等待时间为5秒
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // 3.设置日志级别
        jsc.sparkContext().setLogLevel("WARN");
        // 4.监控该目录下新增的文件，并获取数据
        String file = "DTSparkStreaming\\src\\main\\resources\\data\\";
        JavaDStream<String> lines = jsc.textFileStream(file);

        // 5.返回多条(k,v)，比如(hello,1)
        JavaPairDStream<String, Integer> flatMapToPair = lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterable<Tuple2<String, Integer>> call(String s) throws Exception {
                return Arrays.asList(new Tuple2<String, Integer>(s.trim(), 1));
            }
        });
        // 6.比如(k,w)是(hello,1)
        // (k,v)与(k,w)连接返回(k,(v,w)，比如(hello,(1,1))
        flatMapToPair.join(flatMapToPair).print();
        // 7.启动
        jsc.start();
        // 8.等待被停止
        jsc.awaitTermination();
        // 9.关闭
        jsc.close();

    }
}
