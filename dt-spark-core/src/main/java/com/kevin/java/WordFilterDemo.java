package com.kevin.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author kevin
 * @version 1.0
 * @description     动态统计出现次数最多的单词个数，并过滤掉
 * @createDate 2019/1/2
 */
public class WordFilterDemo {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Demo").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String file = "DTSparkCore\\src\\main\\resources\\records.txt";
        JavaRDD<String> lines = sc.textFile(file);

        // 切分数据
        JavaRDD<String> flatMap = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split("\n"));
            }
        });

        // 对数据初始化值为1
        JavaPairRDD<String, Integer> mapToPair = flatMap.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        // 动态抽样数据，作为统计
        JavaPairRDD<String, Integer> sample = mapToPair.sample(true, 0.5);

        // 将相同的单词次数累加，并排序
        JavaPairRDD<String, Integer> sortMap = sample.reduceByKey(new Function2<Integer, Integer, Integer>() {
            // 使用reduceByKey算子，将相同的单词次数累加
            @Override
            public Integer call(Integer t1, Integer t2) throws Exception {
                return t1 + t2;
            }
        }).mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            // 使用mapToPair算子，将次数作为key，方便排序
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> s) throws Exception {
                return new Tuple2<Integer, String>(s._2, s._1);
            }
        }).sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            // 降序，将重新将单词做为key
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> s) throws Exception {
                return new Tuple2<String, Integer>(s._2, s._1);
            }
        });

        // 获取单词次数最多的一条数据
        List<Tuple2<String, Integer>> take = sortMap.take(1);

        System.out.println("动态统计出现次数最多的单词: " + take);

        // 过滤掉次数最多的一条数据
        JavaPairRDD<String, Integer> filter = sortMap.filter(new Function<Tuple2<String, Integer>, Boolean>() {
            // 判断数据是否为最多的那条，如果是，则过滤掉，true保留，false过滤
            @Override
            public Boolean call(Tuple2<String, Integer> s) throws Exception {
                return !s._1.equals(take.get(0)._1);
            }
        });

        // 遍历输出
        filter.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> s) throws Exception {
                System.out.println(s);
            }
        });

        // 关闭
        sc.close();

    }

}
