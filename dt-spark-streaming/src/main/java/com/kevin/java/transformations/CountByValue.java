package com.kevin.java.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * @author kevin
 * @version 1.0
 * @description     当调用类型为K的元素的DStream时，返回（K,Long）键值对的新DStream，其中键值对的value是key对应的value在DStream中出现的次数
 * @createDate 2019/1/21
 */
public class CountByValue {

    public static void main(String[] args) {

        // 1.创建sparkconf设置作业名称和模式，使用本地模式，最少需要两个线程，一个接收数据，一个处理数据
        SparkConf conf = new SparkConf().setAppName("CountByValue").setMaster("local[2]");
        // 2.创建JavaStreamingContext有两种方式（SparkConf，SparkContext），设置等待时间为5秒
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // 设置日志级别
        jsc.sparkContext().setLogLevel("WARN");
        String fileName = "DTSparkStreaming\\src\\main\\resources\\data";
        // 读取文件数据
        //JavaDStream<String> lines = jsc.textFileStream(fileName);
        // 3.读取socket源的数据
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("Master", 9999);
        // 4.获取到数据并切分
        JavaPairDStream<String, Integer> mapToPair = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                // 比如输入的数据是：kevin 1，则返回(kevin,1)
                return new Tuple2<String, Integer>(
                        s.split(" ")[0].trim(), Integer.valueOf(s.split(" ")[1].trim()));
            }
        });
        // 5.统计k,v相同的次数
        // 如果输入的3次kevin 1，则统计的结果是((kevin,1),3)
        JavaPairDStream<Tuple2<String, Integer>, Long> countByValue = mapToPair.countByValue();
        // 6.打印前1000条结果数据
        countByValue.print(1000);
        // 7.启动
        jsc.start();
        // 8.等待被停止
        jsc.awaitTermination();
        // 9.关闭
        jsc.close();


    }
}
