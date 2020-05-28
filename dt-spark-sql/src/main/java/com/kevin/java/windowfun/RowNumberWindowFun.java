package com.kevin.java.windowfun;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

/**
 * @author kevin
 * @version 1.0
 * @description     row_number()开窗函数：
 * 主要是按照某个字段分组，然后取另一字段的前几个的值，相当于分组取topN
 * row_number() over (partition by xxx order by xx desc) xxx
 * 注意：
 * 如果sql语句里面使用到了开窗函数，那么这个sql语句必须使用HiveContext来执行
 * ，HiveContext默认情况下在本地无法创建
 * @createDate 2019/1/12
 */
public class RowNumberWindowFun {

    public static void main(String[] args) {

        // 1.创建sparkconf，设置作业名称，和设置shuffle分区数（默认是200个）
        SparkConf conf = new SparkConf().setAppName("RowNumberWindowFun").setMaster("local[3]").set("spark.sql.shuffle.partitions", "1");
        // 2.创建SparkContext，是spark作业的唯一通道，同时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 3.创建HiveContext，其是SqlContext子类，专门操作hive中的数据
        HiveContext hiveContext = new HiveContext(sc);
        // 4.进入sparkdb数据库
        hiveContext.sql("use sparkdb");
        // 5.如果sales表存在则删除
        hiveContext.sql("drop table if exists sales");
        // 6.创建sales表
        hiveContext.sql("create table if not exists sales (riqi string" +
                ",leibie string, jine Int) row format delimited fields terminated by '\t'");
        // 7.数据从本地的root/test/sales中拷贝到sales表中
        hiveContext.sql("load data local inpath '/root/test/sales' into table sales");

        // 8.查询开窗函数
        /**
         *  开窗函数格式：[row_number() over (partition by xxx order by xxx desc) as rank]
         *  注意：rank从1开始
         *  以类别分组，按每种类别金额降序排序，显示[日期，种类，金额]结果，如：
         * 1 A 100
         * 2 B 200
         * 3 A 300
         * 4 B 400
         * 5 A 500
         * 6 B 600
         * 排序后：
         * 5 A 500  --rank 1
         * 3 A 300  --rank 2
         * 1 A 100  --rank 3
         * 6 B 600  --rank 1
         * 4 B 400	--rank 2
         * 2 B 200  --rank 3
         */
        DataFrame result = hiveContext.sql("select riqi, leibie, jine from (select riqi, leibie, jine, row_number() over " +
                "(partition by leibie order by jine desc) rank from sales) t where t.rank <= 3");
        // 9.显示前100条数据（默认是显示20条）
        result.show(100);
        // 10.将结果保存到hive表sales_result，如果存在就覆盖
        result.write().mode(SaveMode.Overwrite).saveAsTable("sales_result");
        // 11.关闭
        sc.stop();


    }
}
