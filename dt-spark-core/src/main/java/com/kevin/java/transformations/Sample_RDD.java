package com.kevin.java.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

/**
 * @author kevin
 * @version 1.0
 * @description     Sample随机抽样：withReplacement：false(不会相同)、true(可能会相同)，fraction：抽样的比例，seed：指定随机数生成器种子，种子不同，则序列才会不同
 * @createDate 2018/12/28
 */
public class Sample_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("Sample_RDD").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.读取文件数据
        JavaRDD<Integer> parallelize = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        // 4.随机抽样，为false时抽出就不放回，下次抽到的数据不可能会相同
        JavaRDD<Integer> sampleFalse = parallelize.sample(false, 0.3, System.currentTimeMillis());
        sampleFalse.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println("SampleFalse: "+integer);
            }
        });

        // 5.随机抽样，为true时抽出还会放回，下次抽到数据还可能会相同
        JavaRDD<Integer> sampleTrue = parallelize.sample(true, 0.3, System.currentTimeMillis());
        sampleTrue.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println("SampleTrue: "+integer);
            }
        });

        sc.close();
    }
}
