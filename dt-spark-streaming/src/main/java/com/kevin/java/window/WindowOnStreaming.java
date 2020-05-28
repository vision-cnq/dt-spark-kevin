package com.kevin.java.window;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

/**
 * @author kevin
 * @version 1.0
 * @description     模拟统计最近20秒内 读取的单词的个数
 * @createDate 2019/1/20
 */
public class WindowOnStreaming {

    public static void main(String[] args) {

        // 1.创建sparkconf设置作业名称和模式，使用本地模式，最少需要两个线程，一个接收数据，一个处理数据
        SparkConf conf = new SparkConf().setAppName("WindowOnStreaming").setMaster("local[2]");
        // 2.基于sparkconf创建JavaStreamingContext，设置接收数据间隔为5秒
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // 3.设置checkpoint作为持久化和容错
        jsc.checkpoint("./checkpoint");
        // 4.使用socket数据源，设置节点和端口
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("Master", 9999);
        // 5.获取到socket源传入的数据切分后初始化次数为1
        JavaPairDStream<String, Integer> words = lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterable<Tuple2<String, Integer>> call(String s) throws Exception {
                String[] split = s.split(",");
                ArrayList<Tuple2<String, Integer>> tupleList = new ArrayList<>();
                for (String word : split) {
                    tupleList.add(new Tuple2<String, Integer>(word, 1));
                }
                System.out.println("读取该方法");
                return tupleList;
            }
        });

        // 6.使用优化过的窗口函数，每10秒计算一次最近20秒的值
        JavaPairDStream<String, Integer> window = words.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer - integer2;
            }
            // 设置每10秒计算最近20秒的值
        }, Durations.seconds(20), Durations.seconds(10));

        // 为了方便测试：窗口宽度:20秒，窗口滑动:10秒
        // 输入数据时：第一次输入: a,b,c，第二次输入: d,e,f，第三和第四次不输入
        // 将结果写入文件中，观察30秒时数据是否为null
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String format = sdf.format(new Date());
        // 7.打印数据
        window.print();
        // 8.设置文件名
        window.dstream().saveAsTextFiles("./savedata/prefix"+format,"txt");
        System.out.println("将数据插入文件");
        // 9.启动sparkstreaming
        jsc.start();
        // 10.等待被停止
        jsc.awaitTermination();
        // 11.关闭
        jsc.close();
    }
}
