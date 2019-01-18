package com.kevin.java;

import org.apache.spark.JavaFutureActionWrapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author kevin
 * @version 1.0
 * @description     过滤黑名单
 * transform操作
 * DStream可以通过transform做RDD到RDD的任意操作
 * @createDate 2019/1/16
 */
public class TransForm {

    public static void main(String[] args) {

        // 1.创建sparkconf设置作业名称和模式，使用本地模式，最少需要两个线程，一个接收数据，一个处理数据
        SparkConf conf = new SparkConf().setAppName("TransForm").setMaster("local[2]");

        // 2.创建JavaStreamingContext有两种方式（SparkConf，SparkContext），设置等待时间为5秒
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        jsc.sparkContext().setLogLevel("WARN");

        // 3.新增黑名单人员并放到广播变量中
        List<String> list = Arrays.asList("zhangsan");
        final Broadcast<List<String>> broadcast = jsc.sparkContext().broadcast(list);

        // 4.接受socket数据源
        JavaReceiverInputDStream<String> nameList = jsc.socketTextStream("Master", 9999);

        // 5.切分处理数据，第二个参数是名字
        JavaPairDStream<String, String> pairNameList = nameList.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                // 返回名字和原始数据
                return new Tuple2<String, String>(s.split(" ")[1], s);
            }
        });

        // tranform可以拿到DStream中的RDD做RDD到RDD之间的转换，不需要action算子触发需要返回类型
        // 6.注意：tranform call方法内，拿到RDD算子外的代码在Driver端执行，也可以做到动态改变广播变量
        JavaDStream<String> transform = pairNameList.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {

            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> nameRDD) throws Exception {

                // 过滤数据
                JavaPairRDD<String, String> filter = nameRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        // 得到广播变量，判断数据是否包含需要过滤的数据，false过滤掉
                        return !broadcast.value().contains(tuple._1);
                    }
                });

                // 返回不包含需要过滤的数据
                JavaRDD<String> map = filter.map(new Function<Tuple2<String, String>, String>() {
                    @Override
                    public String call(Tuple2<String, String> tuple) throws Exception {
                        return tuple._2;
                    }
                });

                return map;
            }
        });

        // 7.打印数据
        transform.print();

        // 8.启动程序
        jsc.start();
        // 9.等待被终止
        jsc.awaitTermination();
        // 10.关闭
        jsc.stop();
    }
}
