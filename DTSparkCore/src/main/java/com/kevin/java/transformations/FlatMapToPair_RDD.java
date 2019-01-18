package com.kevin.java.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author kevin
 * @version 1.0
 * @description     flatMapToPair输入一条数据，输出多条数据。一对多：类型是list的Tuple2<K,V>
 * @createDate 2018/12/28
 */
public class FlatMapToPair_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("FlatMapToPair_RDD").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.读取文件数据
        String file = "DTSparkCore\\src\\main\\resources\\words.txt";
        JavaRDD<String> lines = sc.textFile(file);

        // 4.就像Hadoop的Map一样返回K,V类型的list
        JavaPairRDD<String, Integer> flatMapToPair = lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterable<Tuple2<String, Integer>> call(String s) throws Exception {
                /*String[] split = s.split(" ");
                ArrayList<Tuple2<String,Integer>> list = new ArrayList<Tuple2<String,Integer>>();
                for (int i = 0; i < split.length; i++) {
                    list.add(new Tuple2<String, Integer>(split[i],1));
                }*/
                return Arrays.asList(new Tuple2<String, Integer>(s,1));
            }
        });

        // 5.遍历数据
        flatMapToPair.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println("key: " + t._1 + " ,value: " + t._2);
            }
        });

        // 关闭数据
        sc.close();

    }
    
}
