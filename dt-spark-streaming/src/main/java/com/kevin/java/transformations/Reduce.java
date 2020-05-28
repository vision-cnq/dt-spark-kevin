package com.kevin.java.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @author kevin
 * @version 1.0
 * @description     聚合当前流批次的所有rdd
 * 通过使用函数func（接收两个参数并返回一个）聚合DStream的每个RDD中的元素
 * 返回单元素RDD的新DStream，该函数是关联的，以便可以并行计算。
 * reduce处理后返回的是一条数据
 * @createDate 2019/1/21
 */
public class Reduce {

    public static void main(String[] args) {

        // 1.创建sparkconf设置作业名称和模式，使用本地模式，最少需要两个线程，一个接收数据，一个处理数据
        SparkConf conf = new SparkConf().setAppName("Reduce").setMaster("local[2]");
        // 2.创建JavaStreamingContext有两种方式（SparkConf，SparkContext），设置等待时间为5秒
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // 3.设置日志级别
        jsc.sparkContext().setLogLevel("WARN");
        // 4.监控该目录下新增的文件，并获取数据
        String file = "DTSparkStreaming\\src\\main\\resources\\data\\";
        JavaDStream<String> lines = jsc.textFileStream(file);
        // 5.拼接本次和下次的字符串来模拟reduce处理数据
        JavaDStream<String> reduce = lines.reduce(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                return s +"---"+ s2;
            }
        });

        // 6.打印前100条结果数据
        reduce.print(100);
        // 7..启动程序
        jsc.start();
        // 8.等待被终止
        jsc.awaitTermination();
        // 9.关闭
        jsc.stop();
    }
}
