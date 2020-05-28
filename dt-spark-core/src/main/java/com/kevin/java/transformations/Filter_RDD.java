package com.kevin.java.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * @author kevin
 * @version 1.0
 * @description     过滤符合条件的数据，true保留，false过滤
 * @createDate 2018/12/28
 */
public class Filter_RDD {

    public static void main(String[] args) {

        // 1.创建SparkConf，设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("Filter_RDD").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.读取文件数据
        String file = "DTSparkCore\\src\\main\\resources\\test2.txt";
        JavaRDD<String> lines = sc.textFile(file);

        // 4.读取每一行数据，如果包含server则过滤掉
        JavaRDD<String> filter = lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return !s.contains("server");
            }
        });

        // 5.遍历输出
        filter.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        // 6.关闭
        sc.close();
    }
}
