package com.kevin.java.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.Arrays;

/**
 * @author kevin
 * @version 1.0
 * @description     创建json格式的list注册成临时表用sql语句查询
 * @createDate 2019/1/6
 */
public class DataFrameRegisterTempTable {

    public static void main(String[] args) {

        // 1.创建SparkConf作业设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("DataFrameRegisterTempTable").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.根据sc创建SQLContext对象对sql进行分析处理
        SQLContext sqlContext = new SQLContext(sc);

        // 4.创建json格式的list转成rdd
        JavaRDD<String> nameRDD = sc.parallelize(Arrays.asList(
                "{'name':'zhangsan','age':\"18\"}",
                "{\"name\":\"lisi\",\"age\":\"19\"}",
                "{\"name\":\"wangwu\",\"age\":\"20\"}"
        ));
        JavaRDD<String> scoreRDD = sc.parallelize(Arrays.asList(
                "{\"name\":\"zhangsan\",\"score\":\"100\"}",
                "{\"name\":\"lisi\",\"score\":\"200\"}",
                "{\"name\":\"wangwu\",\"score\":\"300\"}"
        ));

        // 5.将json转为转为DataFrame
        DataFrame namedf = sqlContext.read().json(nameRDD);
        namedf.show();
        DataFrame scoredf = sqlContext.read().json(scoreRDD);
        scoredf.show();

        //SELECT t1.name,t1.age,t2.score from t1, t2 where t1.name = t2.name
        //daframe原生api使用，不推荐
		//namedf.join(scoredf, namedf.col("name").$eq$eq$eq(scoredf.col("name")))
		//.select(namedf.col("name"),namedf.col("age"),scoredf.col("score")).show();

        // 6.注册为临时表使用
        namedf.registerTempTable("name");
        scoredf.registerTempTable("score");

        // 7.如果自己写的sql查询得到的DataFrame结果中的列会按照 查询的字段顺序返回
        DataFrame sql = sqlContext.sql("select name.name,name.age,score.score " +
                "from name join score on name.name = score.name");

        // 8.展示数据
        sql.show();

        // 9.关闭
        sc.close();

    }
}
