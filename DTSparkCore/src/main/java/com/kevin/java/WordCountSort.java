package com.kevin.java;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * @author kevin
 * @version 1.0
 * @description     单词计数，并且排序
 * @createDate 2018/12/27
 */
public class WordCountSort {

    public static void main(String[] args) {

        // 1.创建sparkconf，设置模式，并且设置作业名称
        SparkConf conf = new SparkConf().setAppName("WordCountSort").setMaster("local");

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

        // 5.所有的单词初始次数为1
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String,Integer>(word,1);
            }
        });

        // 6.将相同的单词聚合在一起将其中的次数相加
        JavaPairRDD<String, Integer> results = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer value1, Integer value2) throws Exception {
                return value1 + value2;
            }
        });

        // 7.将键值对互换，为了根据值做排序
        JavaPairRDD<Integer, String> temp = results.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
                return new Tuple2<Integer, String>(tuple._2, tuple._1);
            }
        });

        // 8.根据互换的键做排序
        JavaPairRDD<String, Integer> sorted = temp.sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple) throws Exception {
                return new Tuple2<String, Integer>(tuple._2,tuple._1);
            }
        });

        // 9.将结果存到list集合中
        List<Tuple2<String, Integer>> list = sorted.collect();

        // 10.遍历数据并输出（用spark中的foreach）
        sorted.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                System.out.println("word: "+tuple._1+"\tcount: "+tuple._2);
            }
        });

        // 遍历数据并输出（用java的foreach）和（spark的foreach）效果一致
        /*for (Tuple2<String,Integer> t : list) {
            System.out.println(t._1+"--------"+t._2);
        }*/

        // 11.关闭作业
        sc.close();

    }
}
