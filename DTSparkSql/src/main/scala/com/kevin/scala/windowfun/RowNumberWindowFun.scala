package com.kevin.scala.windowfun

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * row_number()开窗函数：
  *   主要是按照某个字段分组，然后取另一字段的前几个的值，相当于分组取topN
  *   row_number() over (partition by xxx order by xx desc) xxx
  * 注意：
  *   如果sql语句里面使用到了开窗函数，那么这个sql语句必须使用HiveContext来执行
  *    ，HiveContext默认情况下在本地无法创建
  */
object RowNumberWindowFun {

  def main(args: Array[String]): Unit = {

    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("RowNumberWindowFun").setMaster("local")
    // 2.创建sc
    val sc = new SparkContext(conf)
    // 3.创建HiveContext
    val hiveContext = new HiveContext(sc)
    // 4.到hive中的sparkdb数据库
    hiveContext.sql("use sparkdb")
    // 5.删除sales表
    hiveContext.sql("drop table if exists sales")
    // 6.新建sales表
    hiveContext.sql("create table if not exists sales (riqi string,leibie string, jine int) row format delimited fields terminated by '\t' ")
    // 7.数据从hdfs中的hdfs://Master:9000/test/sales中拷贝到sales表中
    hiveContext.sql("load data inpath 'hdfs://Master:9000/test/sales' into table sales ")
    // 8.查询开窗函数
    // 开窗函数格式：[row_number() over (partition by xxx order by xxx desc) as rank] 注意：rank从1开始 以类别分组，按每种类别金额降序排序，显示[日期，种类，金额]结果，如：
    val result = hiveContext.sql("select riqi, leibie,jine from (select riqi, leibie, jine, row_number() over (partition by leibie order by jine desc) rank from sales ) t  where t.rank <= 3")

    // 9.显示前一百条数据
    result.show(100)
    // 10.将结果保存到hive中的sales_result表，如果存在就覆盖
    result.write.mode(SaveMode.Overwrite).saveAsTable("sales_result")
    // 11.关闭sc
    sc.stop()

  }

}
