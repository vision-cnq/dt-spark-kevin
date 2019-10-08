package com.kevin.scala.dataframe

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  *  读取mysql数据源
  */
object DataFrameMysqlRead {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("DataFrameMysqlRead").setMaster("local")
    // 配置join或者聚合操作shuffle数据时分区的数量
    conf.set("spark.sql.shuffle.partitions","1")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.创建SQLContext对象对sql进行分析处理
    val sqlContext = new SQLContext(sc)
    val options = Map("url" -> "jdbc:mysql://192.168.171.101:3306/test",
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "root",
      "password" -> "Hadoop01!",
      "dbtable" -> "person")
    // 4.读取Mysql数据库表，加载为DataFrame
    val jdbcDF = sqlContext.read.format("jdbc").options(options).load()
    jdbcDF.show()
    // 5.注册为临时表
    jdbcDF.registerTempTable("temp_person")
    // 6.查询name=kevin
    val result = sqlContext.sql("select id,name,age from temp_person where name = 'kevin' ")
    result.show()
    // 7.关闭sc
    sc.stop()

  }

}
