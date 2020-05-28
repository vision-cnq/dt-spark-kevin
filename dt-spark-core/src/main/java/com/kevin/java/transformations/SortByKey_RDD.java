package com.kevin.java.transformations;

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
 * @author kevin
 * @version 1.0
 * @description     对Key做排序,sortByKey()中的参数为fasle时降序，为true时升序
 * @createDate 2018/12/28
 */
public class SortByKey_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("SortByKey_RDD").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.读取文件数据
        String file = "DTSparkCore\\src\\main\\resources\\words.txt";
        JavaRDD<String> lines = sc.textFile(file);

        // 4.根据空格切分数据
        JavaRDD<String> flatMap = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        // 5.将每个Key的初始化为1
        JavaPairRDD<String, Integer> mapToPair = flatMap.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });

        // 6.将相同的Key的Value值相加
        JavaPairRDD<String, Integer> reduceByKey = mapToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        // 7.将Key，Value的值倒过来，为了对Value的数据做排序
        JavaPairRDD<Integer, String> mapToPair1 = reduceByKey.
                mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {

                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                        return new Tuple2<Integer, String>(t._2, t._1);
                    }
                });

        // 8.对Key做排序,sortByKey()中的参数为fasle时降序，为true时升序
        JavaPairRDD<Integer, String> sortByKey = mapToPair1.sortByKey(false);

        // 9.将Key，Value的值位置再次倒回来
        JavaPairRDD<String, Integer> mapToPair2 = sortByKey.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                return new Tuple2<String,Integer>(t._2,t._1);
            }
        });

        //  10.遍历数据
        mapToPair2.foreach(new VoidFunction<Tuple2<String,Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t);
            }
        });

        // 11.关闭
        sc.close();

    }
}
