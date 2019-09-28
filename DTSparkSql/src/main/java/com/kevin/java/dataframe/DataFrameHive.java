package com.kevin.java.dataframe;

import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import java.util.List;

/**
 * @author kevin
 * @version 1.0
 * @description     如果读取hive中数据，要使用HiveContext
 *  HiveContext.sql(sql)可以操作hive表，还可以操作虚拟表
 * @createDate 2019/1/6
 */
public class DataFrameHive {

    public static void main(String[] args) {

        /*
         * 0.把hive里面的hive-site.xml放到spark/conf目录下
         * 1.启动Mysql
         * 2.启动HDFS
         * 3.启动Hive ./hive
         * 4.初始化HiveContext
         * 5.打包运行
         *
         * ./bin/spark-submit --master yarn-cluster --class com.kevin.java.dataframe.DataFrameHive /root/DTSparkSql.jar
         * ./bin/spark-submit --master yarn-client --class com.kevin.java.dataframe.DataFrameHive /root/DTSparkSql.jar
         */
        // 如果不设置master，则无法在本地运行，需要打包在集群运行
        SparkConf conf = new SparkConf().setAppName("DataFrameHive").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //SparkSession
        // 创建HiveContext，注意，这里，它接收的是SparkContext作为参数，不是JavaSparkContext，
        // 其实也可以使用JavaSparkContext，只不过内部也是做了sc.sc()的操作
        HiveContext hiveContext = new HiveContext(sc);
        DataFrame sql = hiveContext.sql("show databases");
        sql.show();
        sc.close();
    }
/*
    public static void main(String[] args) {

        // 1.创建SparkConf作业设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("DataFrameHive");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.hiveContext是sqlContext的子类，用来操作hive中的数据
        HiveContext hiveContext = new HiveContext(sc.sc());
        hiveContext.sql("show databases").show();

        // 进入sparkhive数据库
        /*hiveContext.sql("use test");

        // 如果存在student_infos表则删除
        hiveContext.sql("drop table if exists student_infos ");
        // 在hive中创建student_infos表
        hiveContext.sql("create table if not exists student_infos (name STRING, age INT)" +
                "row format delimited fields terminated by '\t'");
        // 把本地数据加载到hive中（该操作会将数据拷贝到hdfs），也可以直接操作hdfs中的数据
        hiveContext.sql("load data local inpath '/root/test/student_infos' into table student_infos");

        // 如果存在student_scores表则删除
        hiveContext.sql("drop table if exists student_scores");
        // 在hive中创建student_scores表
        hiveContext.sql("create table if not exists student_scores (name STRING, score INT)" +
                "row format delimited fields terminated by '\t'");
        hiveContext.sql("load data local inpath '/root/test/student_scores' into table student_scores");

        // DataFrame df = hiveContext.table("student_infos");//第二种读取Hive表加载DF方式
        DataFrame goodStudentsDF = hiveContext.sql("SELECT si.name, si.age, ss.score "
                + "FROM student_infos si  JOIN student_scores ss  ON si.name=ss.name  WHERE ss.score>=80");

        // 注册成临时表
        goodStudentsDF.registerTempTable("goodStudentsDF");

        DataFrame result = hiveContext.sql("select * from goodstudent");
        result.show();

        *//**
         * 将结果保存到hive表 good_student_infos
         *//*
        hiveContext.sql("DROP TABLE IF EXISTS good_student_infos");
        goodStudentsDF.write().mode(SaveMode.Overwrite).saveAsTable("good_student_infos");
        DataFrame table = hiveContext.table("good_student_infos");
        Row[] goodStudentRows = table.collect();
        for(Row goodStudentRow : goodStudentRows) {
            System.out.println(goodStudentRow);
        }
        sc.stop();
    }
*/

}
