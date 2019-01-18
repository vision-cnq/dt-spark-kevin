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
 * @description     coalesce减少分区
 *  第二个参数是减少分区的过程中是否产生shuffle，true是产生shuffle，false是不产生shuffle，默认是false.
 *  如果coalesce的分区数比原来的分区数还多，第二个参数设置false，即不产生shuffle,不会起作用。
 *  如果第二个参数设置成true则效果和repartition一样，即coalesce(numPartitions,true) = repartition(numPartitions)
 * @createDate 2018/12/31
 */
public class Coalesce_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("Coalesce_RDD").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.创建list数据
        List<String> list = Arrays.asList(
                "love1","love2","love3",
                "love4","love5","love6",
                "love7","love8","love9",
                "love10","love11","love12"
        );

        // 4.对数据进行分区
        JavaRDD<String> rdd1 = sc.parallelize(list, 3);

        // 5.对数据进行分区处理，但只创建与分区数量相同的对象，并得到当前分区索引
        JavaRDD<String> rdd2 = rdd1.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer integer, Iterator<String> iter) throws Exception {
                ArrayList<String> list = new ArrayList<>();
                while (iter.hasNext()) {
                    list.add("RDD1的分区索引: " + integer + " ,值: " + iter.next());
                }
                return list.iterator();
            }
        }, true);

        // 6.coalesce减少分区
        //JavaRDD<String> coalesceRDD = rdd2.coalesce(2, false);//不产生shuffle
        JavaRDD<String> coalesceRDD = rdd2.coalesce(2, true);//产生shuffle

        //设置分区数大于原RDD的分区数且不产生shuffle，不起作用
        //JavaRDD<String> coalesceRDD = rdd2.coalesce(4,false);
        //System.out.println("coalesceRDD partitions length = "+coalesceRDD.partitions().size());

        //JavaRDD<String> coalesceRDD = rdd2.coalesce(4,true);//设置分区数大于原RDD的分区数且产生shuffle，相当于repartition
        //JavaRDD<String> coalesceRDD = rdd2.repartition(4);

        // 7.重分区，对数据进行分区处理，但只创建与分区数量相同的对象，并得到当前分区索引
        JavaRDD<String> result = coalesceRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer integer, Iterator<String> iter) throws Exception {
                List<String> list = new ArrayList<String>();
                while (iter.hasNext()) {
                    list.add("coalesceRDD的分区索引: " + integer + " ,值为：" + iter.next());

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
