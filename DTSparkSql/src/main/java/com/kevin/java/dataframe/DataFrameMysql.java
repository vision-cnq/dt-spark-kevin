package com.kevin.java.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import java.util.HashMap;
import java.util.Properties;

/**
 * @author kevin
 * @version 1.0
 * @description     读取mysql数据源
 * @createDate 2019/1/8
 */
public class DataFrameMysql {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("DataFrameMysql").setMaster("local");
        // 配置join或者聚合操作shuffle数据时分区的数量
        conf.set("spark.sql.shuffle.partitions","1");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        // 第一种方式，读取Mysql数据库表，加载为DataFrame
        HashMap<String, String> options = new HashMap<>();
        options.put("url","jdbc:mysql://192.168.171.101:3306/sparkdb");
        options.put("driver","com.mysql.jdbc.Driver");
        options.put("user","root");
        options.put("password","Hadoop01!");
        options.put("dbtable","person");

        DataFrame person = sqlContext.read().format("jdbc").options(options).load();
        person.show();
        // 注册为临时表
        person.registerTempTable("person1");


        // 第二种方式，读取Mysql数据库表，加载为DataFrame
        DataFrameReader reader = sqlContext.read().format("jdbc");
        reader.option("url","jdbc:mysql://192.168.171.101:3306/sparkdb");
        reader.option("driver","com.mysql.jdbc.Driver");
        reader.option("user","root");
        reader.option("password","Hadoop01!");
        reader.option("dbtable","score");

        DataFrame score = reader.load();
        score.show();
        score.registerTempTable("score1");

        DataFrame result = sqlContext.sql("select person1.id,person1.name,person1.age,score1.score "
                + "from person1,score1 "
                + "where person1.name = score1.name");
        result.show();

        // 将DataFrame结果保存到Mysql中
        Properties properties = new Properties();
        properties.setProperty("user","root");
        properties.setProperty("password","Hadoop01!");

        result.write().mode(SaveMode.Overwrite)
                .jdbc("jdbc:mysql://192.168.171.101:3306/sparkdb","result",properties);
        System.out.println("-----Finish------");
        sc.stop();
    }

}
