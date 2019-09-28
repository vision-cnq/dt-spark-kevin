package com.kevin.java.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import javax.xml.bind.SchemaOutputResolver;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author kevin
 * @version 1.0
 * @description     对rdd中的每个分区的迭代器进行操作。返回一个可迭代的对象
 *   如果是普通的map，比如一个partition中有1万条数据。ok，那么你的function要执行和计算1万次。
 *   使用MapPartitions操作之后，一个task仅仅会执行一次function，function一次接收所有的partition数据。
 *   只要执行一次就可以了，性能比较高。如果在map过程中需要频繁创建额外的对象
 *   (例如将rdd中的数据通过jdbc写入数据库,map需要为每个元素创建一个链接而mapPartition
 *   为每个partition创建一个链接),则mapPartitions效率比map高的多。
 *   SparkSql或DataFrame默认会对程序进行mapPartition的优化。
 * @createDate 2018/12/30
 */
public class MapPartitions_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("MapPartitions_RDD").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.读取文件数据，并分区
        String file = "DTSparkCore\\src\\main\\resources\\records.txt";
        JavaRDD<String> lines = sc.textFile(file, 3);

        // 4.对数据进行分区处理，但只创建与分区数量相同的对象
        JavaRDD<String> mapPartitions = lines.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
            @Override
            public Iterable<String> call(Iterator<String> t) throws Exception {
                System.out.println("start。。。。");
                List<String> list = new ArrayList();
                while (t.hasNext()) {
                    list.add(t.next());
                }
                System.out.println("遍历存储数据大小: " +list.size());
                System.out.println("end");
                // 返回一个可迭代的对象
                return list;
            }
        });

        // 5.由于mapPartitions是转换算子，在没有行动算子的情况下不会执行，所以，创建行动算子
        mapPartitions.collect();

        // 6.关闭
        sc.close();

    }
}
