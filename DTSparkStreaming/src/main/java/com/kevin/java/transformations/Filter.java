package com.kevin.java.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import sun.text.resources.sr.JavaTimeSupplementary_sr;

/**
 * @author kevin
 * @version 1.0
 * @description     通过func函数过滤返回为false的数据，返回一个新的DStream
 * @createDate 2019/1/21
 */
public class Filter {

    public static void main(String[] args) {

        // 1.创建sparkconf设置作业名称和模式，使用本地模式，最少需要两个线程，一个接收数据，一个处理数据
        SparkConf conf = new SparkConf().setAppName("Count").setMaster("local[2]");
        // 2.创建JavaStreamingContext有两种方式（SparkConf，SparkContext），设置等待时间为5秒
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // 3.设置数据源
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("Master", 9999);
        // 4.过滤为false的数据并显示前100条
        lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                // 为false则过滤掉
                return !s.equals("zhangsan");
            }
        }).print(100);
        // 5.启动
        jsc.start();
        // 6.等待被停止
        jsc.awaitTermination();
        // 7.关闭
        jsc.close();
    }
}
