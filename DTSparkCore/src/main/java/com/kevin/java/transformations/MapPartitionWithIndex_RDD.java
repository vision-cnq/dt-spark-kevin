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
 * @description     mapPartitionsWithIndex与mapPartitions的相似，只是增加了分区的索引
 *  对每个分区的迭代器进行计算，并得到当前分区的索引，返回一个迭代器
 * @createDate 2018/12/30
 */
public class MapPartitionWithIndex_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("MapPartitionWithIndex_RDD").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.创建list数据
        List<String> names = Arrays.asList("java", "scala", ".net","python");

        // 4.这里的第二个参数是设置并行度,也是RDD的分区数，并行度理论上来说设置大小为core的2~3倍
        JavaRDD<String> parallelize = sc.parallelize(names, 3);

        // 5.对数据进行分区处理，但只创建与分区数量相同的对象，并得到当前分区索引
        JavaRDD<String> mapPartitionsWithIndex = parallelize.mapPartitionsWithIndex(
                new Function2<Integer, Iterator<String>, Iterator<String>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterator<String> call(Integer index, Iterator<String> iter)
                            throws Exception {
                        List<String> list = new ArrayList<String>();
                        while(iter.hasNext()){
                            String s = iter.next();
                            list.add(s+"~");
                            System.out.println("分区索引: "+index +",该分区的值: "+s );
                        }
                        // 返回迭代器
                        return list.iterator();
                    }
                }, true); //是否保存分区信息

        // 6.由于mapPartitionsWithIndex是转换算子，在没有行动算子的情况下不会执行，所以，创建行动算子
        mapPartitionsWithIndex.collect();

        // 7.关闭
        sc.close();

    }
}
