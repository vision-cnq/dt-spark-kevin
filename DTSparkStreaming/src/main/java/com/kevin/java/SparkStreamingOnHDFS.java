package com.kevin.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author kevin
 * @version 1.0
 * @description spark standalone or Mesos with cluster deploy mode only:
 * 在提交application的时候，添加 supervise选项会自动启动一个Driver
 * @createDate 2019/1/16
 */
public class SparkStreamingOnHDFS {

    public static void main(String[] args) {

        // 1.创建sparkconf设置作业名称和模式，使用本地模式，最少需要两个线程，一个接收数据，一个处理数据
        SparkConf conf = new SparkConf().setAppName("SparkStreamingOnHDFS").setMaster("local[2]");

        // 如果目录下存在该文件下则会报错
        final String checkpointDirectory = "hdfs://Master:9000/sparkstreaming/CheckPoint2019";
        //final String checkpointDirectory = "./checkpoint";

        //2. 创建JavaStreamingContextFactory抽象类实现其数据
        JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {

            @Override
            public JavaStreamingContext create() {
                return createContext(checkpointDirectory, conf);
            }
        };

        // 3.启动sparkstreaming
        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(checkpointDirectory, factory);
        jsc.sparkContext().setLogLevel("WARN");
        jsc.start();
        jsc.awaitTermination();
        jsc.close();

    }

    // 4.创建checkpoint文件夹
    private static JavaStreamingContext createContext(String checkpointDirectory, SparkConf conf) {

        System.out.println("Creating new context");
        SparkConf sparkConf = conf;
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        ssc.sparkContext().setLogLevel("WARN");
        /*
         checkpoint保存：
            1.配置信息
            2.DStream操作逻辑
            3.job的执行进度
            4.offset
          */
        ssc.checkpoint(checkpointDirectory);

        /*
            监控的是HDFS上的一个目录，监控文件数量的变化
            文件内容如果追加监控不到
            只监控文件夹下新增的文件，减少的文件时监控不到的，文件的内容有改动也监控不到
         */
        // 5.监控该文件下的文件数量变化
        JavaDStream<String> lines = ssc.textFileStream("hdfs://Master:9000/sparkstreaming");
        //JavaDStream<String> lines = ssc.textFileStream("./data");

        // 6.获取每一行数据并切分返回
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        // 7.将处理过的数据初始化次数1
       JavaPairDStream<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s.trim(), 1);
            }
        });

        // 8.将流进来的数据相同的次数相加
        JavaPairDStream<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        // 9.打印数据
        counts.print();
        // 10.返回sparksteaming
        return ssc;
    }
}
