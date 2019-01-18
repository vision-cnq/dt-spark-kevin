package com.kevin.java.transformations;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author kevin
 * @version 1.0
 * @description     Join连接：返回两边有拥有的值
 *                  rightOuterJoin：返回右边拥有的值，若左边没有则返回空
 *                  fullOuterJoin：返回所有的值，左边有，右边就算没有也返回空。右边有，左边就算没有也返回空
 * @createDate 2018/12/28
 */
public class Join_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("Join_RDD").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.将Tuple2类型的list转为成RDD
        JavaPairRDD<Integer, String> nameRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2<Integer, String>(0, "aa"),
                new Tuple2<Integer, String>(1, "a"),
                new Tuple2<Integer, String>(2, "b"),
                new Tuple2<Integer, String>(3, "c")));
        JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 200),
                new Tuple2<Integer, Integer>(3, 300),
                new Tuple2<Integer, Integer>(4, 400)
        ));

        // 4.join,将K相同的V值连接在一起，返回两边都拥有的值
        JavaPairRDD<Integer, Tuple2<String, Integer>> join = nameRDD.join(scoreRDD);

        // 5.遍历join后的数据
        join.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> si) throws Exception {
                System.out.println("join: "+si);
            }
        });

        // 6.右连接，将K相同的V值连接在一起，返回右边拥有的所有数据，左边没有则返回空
        JavaPairRDD<Integer, Tuple2<Optional<String>, Integer>> rightOuterJoin =
                nameRDD.rightOuterJoin(scoreRDD);

        // 7.遍历右连接
        rightOuterJoin.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Optional<String>, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Optional<String>, Integer>> i) throws Exception {
                System.out.println("rightOuterJoin: "+i);
            }
        });

        // 8.full连接，返回所有值，左边有，右边就算没有也返回空。右边有，左边就算没有也返回空
        JavaPairRDD<Integer, Tuple2<Optional<String>, Optional<Integer>>> fullOuterJoin =
                nameRDD.fullOuterJoin(scoreRDD);

        // 9.遍历full连接
        fullOuterJoin.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Optional<String>, Optional<Integer>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Optional<String>, Optional<Integer>>> i) throws Exception {
                System.out.println("fullOuterJoin: " + i);
            }
        });

        // 关闭
        sc.close();
    }
}
