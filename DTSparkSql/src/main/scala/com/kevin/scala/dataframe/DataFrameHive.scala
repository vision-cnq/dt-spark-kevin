package com.kevin.scala.dataframe

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 如果读取hive中数据，要使用HiveContext
  * HiveContext.sql(sql)可以操作hive表，还可以操作虚拟表
  */
object DataFrameHive {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("DataFrameHive").setMaster("spark://Master:7077")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.创建HiveContext
    val hiveContext = new HiveContext(sc)
    // 4.查看所有数据库
    hiveContext.sql("show databases").show()
    // 5.关闭sc
    sc.stop()

  }

}
