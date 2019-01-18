package com.kevin.java;

import com.kevin.java.utils.JDBCWrapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * @author kevin
 * @version 1.0
 * @description     监控hdfs某个目录，如果有新增文件则拆分数据并处理存到集群中的mysql
 * @createDate 2019/1/17
 */
public class SparkStreamingOnHDFSToMysql {

    public static void main(String[] args) {

        // 1.创建sparkconf作业和名称设置模式，设置为两个线程
        SparkConf conf = new SparkConf().setAppName("SparkStreamingOnHDFSToMysql").setMaster("local[2]");

        // 2.作为存放checkpoint的目录，需要不存在
        // checkpoint，1.保持容错，2.保持状态。在开始和结束的时候每个batch都会进行checkpoint
        final String checkpointDirectory = "hdfs://Master:9000/sparkstreaming/CheckPoint2019";

        // 3.JavaStreamingContextFactory的create方法可以创建SparkStreamingContext
        JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {
            // 使用checkpoint持久化数据，如果程序崩溃后重启，可以基于checkpoint恢复到曾经的状态
            @Override
            public JavaStreamingContext create() {
                return createContext(checkpointDirectory,conf);
            }
        };

        // 17.可以从失败中恢复Driver，不过需要制定Driver这个进程运行在Cluster，并且提交应用程序的时候指定supervise
        // 设置了checkpoint，重启程序的时候，getOrCreate()会重新从checkpoint目录中初始化出StreamingContext
        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(checkpointDirectory, factory);
        // 18.设置打印日志的级别
        jsc.sparkContext().setLogLevel("WARN");
        // 19.启动sparkstreaming
        jsc.start();
        // 20.等待被终止
        jsc.awaitTermination();
        // 关闭sparkstreaming
        jsc.close();
    }

    // 4.使用工厂模式构建JavaStreamingContext
    private static JavaStreamingContext createContext(String checkpointDirectory, SparkConf conf) {

        System.out.println("Creating new context");
        SparkConf sparkConf = conf;
        // 5.设置每间隔5秒接收一次数据
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        ssc.checkpoint(checkpointDirectory);

        // 6.指定监控的HDFS目录，每次新增文件都会被sparkstreaming接收到
        JavaDStream<String> lines = ssc.textFileStream("hdfs://Master:9000/test/input_01/");

        // 7.将接收到的数据切分为一个个单词
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                System.out.println("查看数据: "+s);
                return Arrays.asList(s.split(" "));
            }
        });

        // 8.初始化单词次数为1，返回<K,V>,例如: <单词,1>
        JavaPairDStream<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        // 9.将相同的单词次数相加
        JavaPairDStream<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        // 10.遍历数据
        counts.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> pairRdd) throws Exception {

                // 11.将数据分区
                pairRdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> vs) throws Exception {
                        // 12.创建mysql连接
                        JDBCWrapper jdbcWrapper = JDBCWrapper.getJDBCInstance();
                        ArrayList<Object[]> insertParams = new ArrayList<Object[]>();
                        // 13.遍历数据
                        while(vs.hasNext()) {
                            Tuple2<String, Integer> next = vs.next();
                            // 获取单词和次数作为参数保存到集合
                            insertParams.add(new Object[]{next._1,next._2});
                        }
                        System.out.println(insertParams);
                        // 14.使用批处理新增数据
                        jdbcWrapper.doBatch("insert into wordcount values(?,?)",insertParams);
                    }
                });
            }
        });

        // 15.打印执行j结果数据
        counts.print();
        // 16.返回sparkstreaming
        return ssc;
    }

}


