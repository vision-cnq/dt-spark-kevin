package com.kevin.java.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author kevin
 * @version 1.0
 * @description     RePartition重分区，来进行数据紧缩，减少分区数量，将小分区合并为大分区，从而提高效率
 *
 * @createDate 2018/12/31
 */
public class RePartition_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("RePartition_RDD").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.创建list数据
        List<String> list = Arrays.asList(
                "love1","love2","love3",
                "love4","love5","love6",
                "love7","love8","love9",
                "love10","love11","love12"
        );

        // 4.将list数据转为rdd并设置分区
        JavaRDD<String> rdd1 = sc.parallelize(list, 3);

        // 5.对数据进行分区处理，但只创建与分区数量相同的对象，并得到当前分区索引
        JavaRDD<String> rdd2 = rdd1.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

            @Override
            public Iterator<String> call(Integer integer, Iterator<String> iter) throws Exception {

                List<String> list = new ArrayList<String>();
                while (iter.hasNext()) {
                    list.add("RDD1的分区索引: " + integer + " ,值为: " + iter.next());
                }
                return list.iterator();
            }
        },true);

        // 6.重新划分分区
        JavaRDD<String> repartition = rdd2.repartition(1);

        // 7.对数据进行分区处理，但只创建与分区数量相同的对象，并得到当前分区索引
        JavaRDD<String> result = repartition.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer integer, Iterator<String> iter) throws Exception {

                List<String> list = new ArrayList<String>();
                while (iter.hasNext()) {
                    list.add("repartition的分区索引为: " + integer + " ,值为: " + iter.next());
                }
                return list.iterator();
            }
        }, true);

        // 8.遍历
        for (String s : result.collect()) {
            System.out.println(s);
        }

        // 9.关闭
        sc.close();


    }
}
