package com.kevin.java.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * @author kevin
 * @version 1.0
 * @description  cogroup相当于一个key join上所有的value，都给放到一个Iterable里面去
 * 当(k,v)格式和(k,w)格式的DStream使用时，返回一个新的DStream(k,seq [v],seq [w])格式的元祖
 * @createDate 2019/1/21
 */
public class Cogroup {

    public static void main(String[] args) {

        // 1.创建sparkconf设置作业名称和模式，使用本地模式，最少需要两个线程，一个接收数据，一个处理数据
        SparkConf conf = new SparkConf().setAppName("CoGroup").setMaster("local[2]");
        // 2.创建JavaStreamingContext有两种方式（SparkConf，SparkContext），设置等待时间为5秒
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // 3.设置日志级别
        jsc.sparkContext().setLogLevel("WARN");
        // 4.监控该目录下新增的文件，并获取数据
        String file = "DTSparkStreaming\\src\\main\\resources\\data\\";

        JavaDStream<String> lines = jsc.textFileStream(file);
        // 5.返回(k,v)格式的元祖
        JavaPairDStream<String, String> mapToPair = lines.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<String, String>(s.split(" ")[0].trim(),
                        s.split(" ")[1].trim());
            }
        });
        // 6.将相同的key的两个value放到集合
        // (k,v)=(hello,bjsxt),(hello,shsxt) 与 (k,w)=(hello,jnsxt),(hello,bjsxt)
        // 使用cogroup组合之后成(hello,([bjsxt,shsxt],[jnsxt,bjsxt]))
        mapToPair.cogroup(mapToPair).print(100);
        // 7.启动程序
        jsc.start();
        // 8.等待被终止
        jsc.awaitTermination();
        // 9.关闭
        jsc.stop();

    }
}
