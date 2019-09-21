package com.kevin.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author caonanqing
 * @version 1.0
 * @description     单词计数
 * @createDate 2018/12/27
 */
public class WordCount {

    public static void main(String[] args) {

        // 1.创建sparkconf，设置模式，并且设置作业名称
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        String file = "DTSparkCore\\src\\main\\resources\\test.txt";

        // 3.读取文件数据
        JavaRDD<String> text = sc.textFile(file);

        // 4.根据空格切分所有单词
        JavaRDD<String> words = text.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });

        // 5.单词初始化为1
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        // 6.将相同的单词的数值相加
        JavaPairRDD<String, Integer> results = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 7.遍历
        results.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> value) throws Exception {
                System.out.println("word: "+value._1+"\tcount: "+value._2);
            }
        });

        // 8.关闭
        sc.stop();

    }


}
