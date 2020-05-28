package com.kevin.java.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.List;

/**
 * @author kevin
 * @version 1.0
 * @description     Collect将计算的RDD结果回收到Driver端，转为List集合。适用于小量数据
 * @createDate 2018/12/27
 */
public class Collect_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("Collect_RDD").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        String file = "DTSparkCore\\src\\main\\resources\\test.txt";
        // 3.获取文件数据
        JavaRDD<String> linesRDD = sc.textFile(file);

        // 4.使用filter函数过滤kill
        JavaRDD<String> resultRDD = linesRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return !s.contains("kill");
            }
        });

        // 5.将rdd转为集合
        List<String> collect = resultRDD.collect();

        // 6.遍历输出
        for (String s : collect) {
            System.out.println(s);
        }

        // 7.关闭JavaSparkContext
        sc.close();
    }
}
