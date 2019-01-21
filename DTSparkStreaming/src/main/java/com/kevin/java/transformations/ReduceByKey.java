package com.kevin.java.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
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
 * @description     聚合相同key的value
 * 当(k,v)格式的DStream被调用时，返回一个新的(k,v)格式的DStream，其中使用给定的reduce函数聚合每个键的值
 * @createDate 2019/1/21
 */
public class ReduceByKey {

    public static void main(String[] args) {

        // 1.创建sparkconf设置作业名称和模式，使用本地模式，最少需要两个线程，一个接收数据，一个处理数据
        SparkConf conf = new SparkConf().setAppName("ReduceByKey").setMaster("local[2]");
        // 2.创建JavaStreamingContext有两种方式（SparkConf，SparkContext），设置等待时间为5秒
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // 3.设置日志级别
        jsc.sparkContext().setLogLevel("WARN");
        // 4.监控该目录下新增的文件，并获取数据
        String file = "DTSparkStreaming\\src\\main\\resources\\data\\";
        JavaDStream<String> lines = jsc.textFileStream(file);
        // 5.使用reduceBykey必须将JavaDstream转换成(k,v)格式
        JavaPairDStream<String, Integer> flatMapToPair = lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            // 初始化key的次数为1
            @Override
            public Iterable<Tuple2<String, Integer>> call(String s) throws Exception {
                return Arrays.asList(new Tuple2<String, Integer>(s, 1));
            }
        });
        // 6.聚合相同key的value值，并显示前100条结果数据
        flatMapToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        }).print(100);

        // 7..启动程序
        jsc.start();
        // 8.等待被终止
        jsc.awaitTermination();
        // 9.关闭
        jsc.stop();

    }
}
