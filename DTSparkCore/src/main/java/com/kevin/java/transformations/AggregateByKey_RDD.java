package com.kevin.java.transformations;

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
 * @description 对PairRDD中相同的Key值进行聚合操作，在聚合过程中同样使用了一个中立的初始值。和aggregate函数类似，
 * aggregateByKey返回值的类型不需要和RDD中value的类型一致。因为aggregateByKey是对相同Key中的值进行聚合操作，
 * 所以aggregateByKey函数最终返回的类型还是PairRDD，对应的结果是Key和聚合后的值，而aggregate函数直接返回的是非RDD的结果
 * @createDate 2019/1/2
 */
public class AggregateByKey_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("AggregateByKey_RDD").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.将Tuple2的数据转为rdd并设置分区
        JavaPairRDD<Integer, Integer> rdd =sc.parallelizePairs(Arrays.asList(
                new Tuple2<Integer, Integer>(1, 1),
                new Tuple2<Integer, Integer>(2, 2),
                new Tuple2<Integer, Integer>(1, 3),
                new Tuple2<Integer, Integer>(2, 4),
                new Tuple2<Integer, Integer>(3, 5),
                new Tuple2<Integer, Integer>(3, 6)
        ),2);

        // 4.对分区的数据进行处理，但只创建与分区数量相同的对象，并得到当前分区索引
        rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer, Integer>>, Iterator<Tuple2<Integer, Integer>>>() {

            @Override
            public Iterator<Tuple2<Integer, Integer>> call(Integer index, Iterator<Tuple2<Integer, Integer>> iter) throws Exception {
                List<Tuple2<Integer, Integer>> list = new ArrayList<Tuple2<Integer, Integer>>();
                while (iter.hasNext()) {
                    System.out.println("partitions: " + index + " ,value: " + iter.next());
                }
                return list.iterator();
            }
        }, true).collect();
        System.out.println("-----------");

        // 5.设置中立的值并参与计算，先对相同分区的数据进行处理，再对不同分区相同key的数据进行处理
        JavaPairRDD<Integer, Integer> aggregateByKey = rdd.aggregateByKey(80, new Function2<Integer, Integer, Integer>() {
            // 合并在同一个partition中的值，v1的数据类型为zeroValue的数据类型，v2的数据类型为原value的数据类型
            // 对同一个partition中的数据进行处理
            @Override
            public Integer call(Integer t1, Integer t2) throws Exception {
                System.out.println("seq: " + t1 + "\t " + t2);
                return t1+t2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            // 合并不同partition中的值，v1，v2的数据类型为zeroValue的数据类型
            // 对不同partition中相同的key进行处理
            @Override
            public Integer call(Integer t1, Integer t2) throws Exception {
                System.out.println("comb: " + t1 + "\t" + t2);
                return t1 + t2;
            }
        });

        // 6.转为集合
        List<Tuple2<Integer, Integer>> collect = aggregateByKey.collect();

        // 7.遍历集合中的数据
        for ( Tuple2<Integer,Integer> tuple2:collect) {
            System.out.println(tuple2._1+"\t"+tuple2._2);
        }

        // 8.关闭
        sc.close();
    }
}
