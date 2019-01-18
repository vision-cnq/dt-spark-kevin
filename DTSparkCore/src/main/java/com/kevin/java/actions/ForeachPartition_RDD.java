package com.kevin.java.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Iterator;

/**
 * @author kevin
 * @version 1.0
 * @description     在获取文件数据的时候指定最小分区，并且在遍历的时候使用foreachPartition进行分区输出
 * @createDate 2018/12/27
 */
public class ForeachPartition_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("ForeachPartition_RDD").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        String file = "DTSparkCore\\src\\main\\resources\\test.txt";
        // 3.设置最小分区
        JavaRDD<String> lines = sc.textFile(file,3);

        // 4.分区遍历，分为三区，如果使用foreach则达不到分区效果。
        // 如果读取数据的时候不指定最小分区，默认只有一个分区
        lines.foreachPartition(new VoidFunction<Iterator<String>>() {
            @Override
            public void call(Iterator<String> t) throws Exception {
                System.out.println("分区");
                while (t.hasNext()) {
                    System.out.println(t.next());
                }
            }
        });

        // 5.关闭
        sc.close();
    }
}
