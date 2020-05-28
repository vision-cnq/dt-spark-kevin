package com.kevin.java.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * @author kevin
 * @version 1.0
 * @description     读取json文件并保存成parquet文件和加载parquet文件
 * @createDate 2019/1/6
 */
public class DataFrameParquet {

    public static void main(String[] args) {

        // 1.创建SparkConf作业设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("DataFrameParquet").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.根据sc创建SQLContext用于sql的分析处理
        SQLContext sqlContext = new SQLContext(sc);

        // 4.读取文件数据
        String file = "DTSparkSql\\src\\main\\resources\\json";
        JavaRDD<String> jsonRDD = sc.textFile(file);

        // 5.读取json形式的文件并转为DataFrame
        DataFrame df = sqlContext.read().json(jsonRDD);
        // DataFrame json = sqlContext.read().format("json").load("./spark/json");
        // json.show();

        // 6.将DataFrame保存成parquet文件
        // SaveMode指定存储文件时的保存模式：Overwrite：覆盖，Append：追加，ErrorIfExists：如果存在就报错，Ignore：若果存在就忽略
        // 保存成parquet文件有以下两种方式
        df.write().mode(SaveMode.Overwrite).format("parquet").save("./sparksql/parquet");
        // df.write().mode(SaveMode.Ignore).parquet("./sparksql/parquet");

        // 7.加载parquet文件成DataFrame
        // 记载parquet文件有以下两种方式
        DataFrame load = sqlContext.read().format("parquet").load("./sparksql/parquet");
        // DataFrame load = sqlContext.read().parquet("./sparksql/parquet");

        // 8.查看表中所有数据
        load.show();
        // 9.关闭
        sc.close();
    }
}
