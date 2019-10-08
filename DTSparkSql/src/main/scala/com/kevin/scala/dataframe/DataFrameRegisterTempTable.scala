package com.kevin.scala.dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 创建json格式的list注册成临时表用sql语句查询
  */
object DataFrameRegisterTempTable {

  def main(args: Array[String]): Unit = {

    // 1.创建sparkconf
    val conf = new SparkConf().setAppName("DataFrameRegisterTempTable").setMaster("local")
    // 2.创建sc
    val sc = new SparkContext(conf)
    // 3.创建sqlcontext
    val sqlContext = new SQLContext(sc)
    // 4.创建json格式的list转成rdd
    val name = sc.parallelize(
      List("{\"name\":\"zhangsan\",\"age\":\"18\"}",
        "{\"name\":\"lisi\",\"age\":\"19\"}",
        "{\"name\":\"wangwu\",\"age\":\"20\"}"))

    val score = sc.parallelize(
      List("{\"name\":\"zhangsan\",\"score\":\"100\"}",
      "{\"name\":\"lisi\",\"score\":\"200\"}",
      "{\"name\":\"wangwu\",\"score\":\"300\"}"))

    // 5.将json转为转为DataFrame
    val namedf = sqlContext.read.json(name)
    namedf.show()
    val scoredf = sqlContext.read.json(score)
    scoredf.show()

    // 6.注册为临时表使用
    namedf.registerTempTable("name")
    scoredf.registerTempTable("score")
    // 7.如果自己写的sql查询得到的DataFrame结果中的列会按照 查询的字段顺序返回
    sqlContext.sql("select a.name,a.age,b.score from name a join score b on a.name = b.name").show()
    // 8.关闭sc
    sc.stop()

  }

}
