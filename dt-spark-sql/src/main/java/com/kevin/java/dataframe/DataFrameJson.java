package com.kevin.java.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * @author kevin
 * @version 1.0
 * @description     读取json格式文件转为DataFrame表形式分析处理
 *
 *  json文件中不能嵌套json格式的内容
 *  1.读取json格式两种方式
 *  2.df.show()默认显示前20行，使用df.show(行数)显示多行
 *  3.df.javaRDD/(scala df.rdd)将DataFrame转换成RDD
 *  4.df.printSchema()显示DataFrame中Schema信息
 *  5.dataFrame自带的API操作DataFrame，一般不使用
 *  6.使用sql查询，先将DataFrame注册成临时表：df.registerTempTable("jtable")，
 *  再使用sql,怎么使用sql?sqlContext.sql("sql语句")
 *  7.不能加载嵌套的json文件
 *  8.df加载过来之后将列安装ascii排序
 * @createDate 2019/1/6
 */
public class DataFrameJson {

    public static void main(String[] args) {

        // 1.创建SparkConf作业设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("DataFrameJson").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.根据sc创建SQLContext对象对sql进行分析处理
        SQLContext sqlContext = new SQLContext(sc);

        // 4.读取文件数据
        String file = "DTSparkSql\\src\\main\\resources\\json";

        // 5.读取json文件并转成DataFrame表
        DataFrame df = sqlContext.read().json(file);

        // 6.获取表中所有数据
        df.show();

        // 7.使用DataFrame自带的API操作DataFrame
        // select name from table
        df.select("name").show();

        // select name, age+10 as addage from table
        df.select(df.col("name"),df.col("age").plus(10).alias("addage")).show();

        // select name ,age from table where age>19
        df.select(df.col("name"),df.col("age")).where(df.col("age").gt(19)).show();

        // select age,count(*) from table group by age
        df.groupBy(df.col("age")).count().show();

        // 8.将DataFrame注册成临时表使用sql语句操作
        df.registerTempTable("jtable");

        DataFrame sql = sqlContext.sql("select age,count(*) as gg from jtable group by age");
        sql.show();
        DataFrame sql2 = sqlContext.sql("select name,age from jtable");
        sql2.show();

        // 关闭
        sc.close();
    }
}
