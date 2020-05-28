package com.kevin.java;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author kevin
 * @version 1.0
 * @description     自定义分区器
 * @createDate 2019/1/2
 */
public class PartitionerDemo {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("PartitionerDemo").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 将Tuple2的数据转为rdd
        JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(Arrays.asList(
                new Tuple2<Integer, String>(1,"java"),
                new Tuple2<Integer, String>(2,"scala"),
                new Tuple2<Integer, String>(3,"python"),
                new Tuple2<Integer, String>(4,"c#"),
                new Tuple2<Integer, String>(5,"c"),
                new Tuple2<Integer, String>(6,"c++")
        ),2);

        // 自定义分区前
        rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer, String>>, Iterator<String>>() {

            @Override
            public Iterator<String> call(Integer index, Iterator<Tuple2<Integer, String>> iter) throws Exception {
                List<String> list = new ArrayList<String>();
                while(iter.hasNext()){
                    System.out.println("自定义分区前 partitionID = "+index+" , value = "+iter.next());
                }
                return list.iterator();
            }
        },true).collect();

        System.out.println("------");

        JavaPairRDD<Integer, String> partitionRDD = rdd.partitionBy(new Partitioner() {
            // 设置分区数
            @Override
            public int numPartitions() {
                return 2;
            }

            // 自定义分区规则，对输入的key做计算，然后返回该key的分区ID，范围一定是0到numPartitions-1
            @Override
            public int getPartition(Object key) {
                int i = (int) key;
                if (i % 2 == 0) {
                    return 0;
                }
                return 1;
            }
        });

        // 自定义分区后
        partitionRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer, String>>, Iterator<String>>() {

            @Override
            public Iterator<String> call(Integer index, Iterator<Tuple2<Integer, String>> iter) throws Exception {
                List<String> list = new ArrayList<String>();
                while(iter.hasNext()){
                    System.out.println("自定义分区后 partitionID = "+index+" , value = "+iter.next());
                }
                return list.iterator();
            }
        },true).collect();

        sc.close();

    }
}
