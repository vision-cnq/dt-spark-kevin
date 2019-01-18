package com.kevin.java;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author kevin
 * @version 1.0
 * 1.local的模拟线程必须大于等于2，至少一条线程被receiver（接受数据的线程）占用，另一个线程是job执行
 * 2.Durations时间的设置，就是我们能接受的延迟度，这个我们需要根据集群的资源情况以及监控每一个job的执行时间来调节出最佳时间
 * 3.创建JavaStreamingContext有两种方式（SparkConf，SparkContext）
 * 4.业务逻辑完成后，需要有一个output operator
 * 5.JavaStreamingContext.start() streaming框架启动之后是不能再次添加业务逻辑
 * 6.JavaStreamingContext.stop() 无参的stop方法会将sparkContext一起关闭，stop(false)，默认为true，会一起关闭
 * 7.JavaStreamingContext.stop()停止之后是不能去调用start
 *
 * foreachRDD   算子注意：
 * 1.foreachRDD是DStream中output 类算子
 * 2.foreachRDD可以遍历得到DStream中的RDD，可以在这个算子内对RDD使用RDD的Transformation类算子进行转化，但一定要使用rdd的Action类算子触发执行
 * 3.foreachRDD可以得到DStream中的RDD，在这个算子内，RDD算子外执行的代码是在Driver端执行，RDD算子内的代码是在Executor中执行。
 *
 * @description     实时计算单词统计
 * @createDate 2019/1/14
 */
public class SparkStreamingTest {

    public static void main(String[] args) {

        // 1.创建sparkconf设置作业名称和模式，使用本地模式，最少需要两个线程，一个接收数据，一个处理数据
        SparkConf conf = new SparkConf().setAppName("SparkStreamingTest").setMaster("local[2]");

        // 2.创建JavaStreamingContext有两种方式（SparkConf，SparkContext）
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        // 3.设置流计算入口，接收时间为5秒
        JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(5));

        // 4.设置接收数据源类型，使用的socket，设置其节点和端口
        // 在Master启动nc -lk 9999 ，输入数据这里就能接收到了
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("Master", 9999);

        // 5.进入一行数据，经过处理出去多条数据
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                System.out.println("DStream flatMap...");
                return Arrays.asList(s.split(" "));
            }
        });

        // 6.初始化单词的次数为1
        JavaPairDStream<String, Integer> pairwords = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        // 7.将单词相同的单词次数相加
        JavaPairDStream<String, Integer> reduceByKey = pairwords.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        //
        // reduceByKey.print();
        // 8.打印,foreachRDD可以拿到DStream中的RDD，对拿到的RDD可以使用transformation类算子转换。
        // 要对拿到的RDD使用action算子触发执行，否则，foreachRDD不会被执行
        // foreachRDD中的call方法内，拿到的RDD的算子外，代码是在Driver端执行
        reduceByKey.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> s) throws Exception {
                System.out.println("Driver...");
                // 创建广播变量
                SparkContext context = s.context();
                JavaSparkContext sparkContext = new JavaSparkContext(context);
                // 如果有数据需要一直使用，其中数据需要改动，就需要重启程序
                // 但是重启程序不太好，所以可以将数据放在文件，读取文件数据，创建成list。这样就不需要重启程序，
                // 因为会根据设置的时间后去读取文件，比如现在是5秒，会在5秒后重新读取文件获取到需要的值
                Broadcast<String> broadcast = sparkContext.broadcast("广播变量: hello");
                String value = broadcast.value();
                System.out.println(value);

                JavaPairRDD<String, Integer> mapToPair = s.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple2) throws Exception {
                        System.out.println("Executor...");
                        return new Tuple2<String, Integer>(tuple2._1 + "~", tuple2._2);
                    }
                });
                mapToPair.foreach(new VoidFunction<Tuple2<String, Integer>>() {
                    @Override
                    public void call(Tuple2<String, Integer> tuple2) throws Exception {
                        System.out.println(tuple2);
                    }
                });

            }
        });
        // 启动
        jsc.start();
        // 等待被终结
        jsc.awaitTermination();
        // 停止，该方法一般是使用不上，因为程序是7*24小时不间断运行的
        jsc.stop();

    }
}
