package com.kevin.java.output;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;

/**
 * @author kevin
 * @version 1.0
 * @description     可以保存到HDFS或本地路径
 *      saveAsObjectFiles(prefix, [suffix])：
 * 将此Dstream的内容保存为序列化的java 对象SequenceFiles ，
 * 每批次数据产生的文件名称格式基于：prefix和suffix: "prefix-TIME_IN_MS[.suffix]".
 * @createDate 2019/1/21
 */
public class SaveAsObjectFiles {

    public static void main(String[] args) {

        // 1.创建sparkconf设置作业名称和模式，使用本地模式，最少需要两个线程，一个接收数据，一个处理数据
        SparkConf conf = new SparkConf().setAppName("SaveAsObjectFiles").setMaster("local[2]");
        // 2.创建JavaStreamingContext有两种方式（SparkConf，SparkContext），设置等待时间为5秒
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // 3.设置日志级别
        jsc.sparkContext().setLogLevel("WARN");
        // 4.监控该目录下新增的文件，并获取数据
        String file = "DTSparkStreaming\\src\\main\\resources\\";
        JavaDStream<String> lines = jsc.textFileStream(file+"data\\");
        // 5.输入一条数据切分处理后输出多条数据
        JavaDStream<String> flatMap = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        // 6.保存到本地路径
        flatMap.dstream().saveAsObjectFiles(file+"myObjData","su");
        // 保存到HDFS
        //flatMap.dstream().saveAsObjectFiles("hdfs://Master:9000/log/obj","su");

        // 7.启动
        jsc.start();
        // 8.等待被停止
        jsc.awaitTermination();
        // 9.关闭
        jsc.close();
    }
}
