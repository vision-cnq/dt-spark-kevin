package com.kevin.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @author kevin
 * @version 1.0
 * @description foreachRDD是output类算子
 * 对于从流中RDD应用func的最通用的一个output操作
 * 该功能应将每个RDD中的数据推送到外部系统，例如将RDD保存到文件中，或将其通过网络写入数据库
 *
 * 注意：如果使用foreachRDD算子，必须要对抽取出来的RDD执行action类算子，代码才能正常的运行
 *
 * @createDate 2019/1/16
 */
public class foreachRDD {

    public static void main(String[] args) {

        // 1.创建sparkconf，设置作业名称和模式，使用本地模式，本地模式最少也要2个线程，一个接收数据，一个处理数据
        SparkConf conf = new SparkConf().setAppName("foreachRDD").setMaster("local[2]");

        // 创建JavaStreamingContext有两种方式（SparkConf，SparkContext）
        // JavaSparkContext sc = new JavaSparkContext(conf);

        // 2.根据sparkconf创建javastreamingcontext（也可以使用SparkContext），设置等待时间为5秒
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 3.读取Master节点9999端口中的socket流的数据
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("Master", 9999);

        // 4.foreachRDD是执行算子，需要使用执行算子触发执行
        lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> rdd) throws Exception {
                // 遍历输出数据
                rdd.foreach(new VoidFunction<String>() {
                    @Override
                    public void call(String s) throws Exception {
                        System.out.println(s);
                    }
                });
            }
        });

        // 5.启动sparkStreaming
        jsc.start();
        // 6.等待被终结
        jsc.awaitTermination();
        // 7.关闭sparkstreaming，一般该方法不会被执行
        jsc.close();


    }
}
