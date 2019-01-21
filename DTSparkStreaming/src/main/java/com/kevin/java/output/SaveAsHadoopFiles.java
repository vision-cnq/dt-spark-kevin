package com.kevin.java.output;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
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
 * @description     保存到HDFS
 *
 * saveAsHadoopFiles(prefix, [suffix])：
 * 将此DStream的内容另存为Hadoop文件。每批次数据产生的文件名称格式基于：prefix和suffix: "prefix-TIME_IN_MS[.suffix]".
 *
 * @createDate 2019/1/21
 */
public class SaveAsHadoopFiles {

    public static void main(String[] args) {

        // 1.创建sparkconf设置作业名称和模式，使用本地模式，最少需要两个线程，一个接收数据，一个处理数据
        SparkConf conf = new SparkConf().setAppName("SaveAsHadoopFiles").setMaster("local[2]");
        // 2.创建JavaStreamingContext有两种方式（SparkConf，SparkContext），设置等待时间为5秒
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // 3.设置日志级别
        jsc.sparkContext().setLogLevel("WARN");
        // 4.监控该目录下新增的文件，并获取数据
        String file = "DTSparkStreaming\\src\\main\\resources\\";
        JavaDStream<String> lines = jsc.textFileStream(file + "data\\");
        // 5.输入一条数据切分处理后输出多条数据
        JavaDStream<String> flatMap = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });
        // 6.将数据转为元祖类型
        JavaPairDStream<String, Integer> mapToPair = flatMap.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s.trim(), 1);
            }
        });

        // 7.存hdfs上路径
        mapToPair.saveAsHadoopFiles("hdfs://Master:9000/log/prefix", "suffix", Text.class, IntWritable.class, TextOutputFormat.class);

        // 保存到本地路径
        //mapToPair.saveAsHadoopFiles(file+"mydata","suffix",Text.class,IntWritable.class,TextOutputFormat.class);

        // 8.启动
        jsc.start();
        // 9.等待被停止
        jsc.awaitTermination();
        // 10.关闭
        jsc.close();
    }
}
