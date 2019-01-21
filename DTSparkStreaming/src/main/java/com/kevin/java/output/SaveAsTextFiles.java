package com.kevin.java.output;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;

import java.util.Arrays;

/**
 * @author kevin
 * @version 1.0
 * @description     保存到本地路径
 *
 * 将此DStream的内容另存为文本文件，每批次数据产生的文件名格式基于：prefix和suffix："prefix-TIME_IN_MS[.suffix]"
 * saveAsTextFile是调用saveAsHadoopFile实现的
 *
 * spark中普通rdd可以直接只用saveAsTextFile(path)的方式，保存到本地，
 * 但是此时DStream的只有saveAsTextFiles()方法，没有传入路径的方法
 * 其参数只有prefix，suffix
 *
 * 其实：DStream中的saveAsTextFiles方法中又调用了rdd中的saveAsTextFile方法，我们需要将path包含在prefix中
 *
 * @createDate 2019/1/21
 */
public class SaveAsTextFiles {

    public static void main(String[] args) {

        // 1.创建sparkconf设置作业名称和模式，使用本地模式，最少需要两个线程，一个接收数据，一个处理数据
        SparkConf conf = new SparkConf().setAppName("SaveAsTextFiles").setMaster("local[2]");
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

        // 6.保存在当前路径中mydata路径下，以prefix开头，以suffix结尾
        DStream<String> dstream = flatMap.dstream();
        // 7.prefix是文件路径加文件名，suffix是文件后缀名
        dstream.saveAsTextFiles(file+"mydata","xxx");
        // 8.启动
        jsc.start();
        // 9.等待被停止
        jsc.awaitTermination();
        // 10.关闭
        jsc.close();
    }
}
