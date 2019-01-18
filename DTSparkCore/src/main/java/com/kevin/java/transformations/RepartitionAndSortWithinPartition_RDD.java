package com.kevin.java.transformations;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

/**
 * @author kevin
 * @version 1.0
 * @description  如果需要在repartition重分区之后，还要进行排序则推荐使用该算子，
 * 效率比使用repartition加sortBykey高，排序是在shuffle过程中进行，一边shuffle，一边排序
 * @createDate 2019/1/2
 */
public class RepartitionAndSortWithinPartition_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("RepartitionAndSortWithinPartition_RDD").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.将Tuple2的数据转为rdd并设置分区
        JavaPairRDD<Integer, Integer> rdd =sc.parallelizePairs(Arrays.asList(
                new Tuple2<Integer, Integer>(1, 1),
                new Tuple2<Integer, Integer>(2, 2),
                new Tuple2<Integer, Integer>(1, 3),
                new Tuple2<Integer, Integer>(5, 4),
                new Tuple2<Integer, Integer>(3, 5),
                new Tuple2<Integer, Integer>(7, 6)
        ),1);

        // 4.先重新分区，再对其进行排序
        JavaPairRDD<Integer, Integer> rdd1 = rdd.repartitionAndSortWithinPartitions(new Partitioner() {
            // 设置分区数据
            @Override
            public int numPartitions() {
                return 3;
            }

            // 对输入的key做计算，然后返回该key的分区ID，范围一定是0到numPartitions-1
            @Override
            public int getPartition(Object key) {
                return Integer.valueOf(key + "") % numPartitions();
            }
            // 排序的规则，可以使用默认，也可以自定义排序规则
        }, new MySort());
        System.out.println("rdd1.partitions().size(): " +  rdd1.partitions().size());

        // 5.对数据进行分区处理，但只创建与分区数量相同的对象，并得到当前分区索引
        rdd1.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer, Integer>>, Iterator<Tuple2<Integer,Integer>>>() {

            @Override
            public Iterator<Tuple2<Integer, Integer>> call(Integer v1, Iterator<Tuple2<Integer, Integer>> v2) throws Exception {
                while (v2.hasNext()) {
                    System.out.println("partitionId: " + v1 + " ,value: " + v2.next());
                }
                return v2;
            }
        },true).count();

        // 6.关闭
        sc.close();
    }
}

// 序列化并重写比较方法
class MySort implements Serializable, Comparator<Integer> {

    @Override
    public int compare(Integer o1, Integer o2) {
        return o2-o1;
    }
}
