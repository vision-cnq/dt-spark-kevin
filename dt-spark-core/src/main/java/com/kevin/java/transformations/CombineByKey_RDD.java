package com.kevin.java.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author kevin
 * @version 1.0
 * @description combineByKey是针对不同partition进行操作的。第一个参数用于数据初始化，第二个参数是同个partition内combine操作函数，
 *         // 第三个参数是在所有partition都combine完后，对所有临时结果进行combine操作的函数。
 * @createDate 2018/12/31
 */
public class CombineByKey_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("CombineByKey_RDD").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.将Tuple2的数据转为rdd,并分区
        JavaPairRDD<String, Integer> parallelizePairs =sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("a", 1),
                new Tuple2<String, Integer>("b", 2),
                new Tuple2<String, Integer>("b", 3),
                new Tuple2<String, Integer>("a", 4),
                new Tuple2<String, Integer>("b", 5),
                new Tuple2<String, Integer>("c", 6)
        ),2);

        // 4.对数据进行分区处理，但只创建与分区数量相同的对象，并得到当前分区索引
        parallelizePairs.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<String, Integer>>, Iterator<Tuple2<String, Integer>>>() {

            @Override
            public Iterator<Tuple2<String, Integer>> call(Integer integer, Iterator<Tuple2<String, Integer>> iter) throws Exception {
                List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
                while (iter.hasNext()) {
                    Tuple2<String, Integer> next = iter.next();
                    System.out.println("RDD1分区索引: " + integer + " ,value: " + next);
                    list.add(next);
                }
                return list.iterator();
            }
        }, true).collect();
        System.out.println("---------------");

        // 5.combineByKey是针对不同partition进行操作的。第一个参数用于数据初始化，第二个参数是同个partition内combine操作函数，
        // 第三个参数是在所有partition都combine完后，对所有临时结果进行combine操作的函数。
        JavaPairRDD<String, Integer> combineByKey = parallelizePairs.combineByKey(new Function<Integer, Integer>() {
            // a.初始端
            // 把当前分区的第一个值当做v1，这里给每个partition相当于初始化值，如果当前partition的key已初始化值则执行第二个函数
            @Override
            public Integer call(Integer v1) throws Exception {
                System.out.println("第一个Function2：" + v1);
                return v1;
            }
        }, new Function2<Integer, Integer, Integer>() {
            // b.Combiner聚合逻辑
            // 合并在同一个partition中的值
            @Override
            public Integer call(Integer s, Integer v) throws Exception {
                System.out.println("第二个Function2：" + (s + v));
                return s + v;
            }
        }, new Function2<Integer, Integer, Integer>() {
            // c.reduce聚合逻辑
            // 合并不同的partition中的值，该函数在前面两个函数已经执行完之后才会执行
            @Override
            public Integer call(Integer s1, Integer s2) throws Exception {
                System.out.println("第三个Function2：" + (s1 + s2));
                return s1 + s2;
            }
        });

        // 6.遍历数据
        combineByKey.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> s) throws Exception {
                System.out.println(s);
            }
        });

        // 7.关闭
        sc.close();

    }
}

