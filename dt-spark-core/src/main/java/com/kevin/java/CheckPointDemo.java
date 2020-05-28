package com.kevin.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author kevin
 * @version 1.0
 * @description     Checkpoint 是为了最大程度保证绝对可靠的复用 RDD 计算数据的 Spark 的高级功能，
 * 通过 Checkpoint 我们通过把数据持久化到 HDFS 上来保证数据的最大程度的安全性
 *      使用场景：多个rdd需要重复被使用的时候，可以将其cache缓存，
 *      然后为了防止某些故障导致数据的遗失，使用CheckPoint做为容错
 * @createDate 2019/1/2
 */
public class CheckPointDemo {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("CheckPointDemo").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        // checkpoint的存放路径
        sc.setCheckpointDir("hdfs://192.168.171.100:9000/test/checkpoint_file/");
        String file = "DTSparkCore\\src\\main\\resources\\records.txt";
        JavaRDD<String> lines = sc.textFile(file);

        // 对每行数据设置初始化值为1，并以K,V形式返回
        JavaPairRDD<String, Integer> flatMapToPair = lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterable<Tuple2<String, Integer>> call(String s) throws Exception {
                return Arrays.asList(new Tuple2<String, Integer>(s, 1));
            }
        });

        // 将数据放到缓存
        JavaPairRDD<String, Integer> rdd = flatMapToPair.cache();

        // 为rdd设置检查点
        rdd.checkpoint();

        // cache和checkpoint都是转换算子，需要执行算子来执行该操作
        rdd.collect();
        System.out.println("isCheckpointed:" + rdd.isCheckpointed());
        System.out.println("checkpoint:" + rdd.getCheckpointFile());
        // 关闭
        sc.close();
    }
}
