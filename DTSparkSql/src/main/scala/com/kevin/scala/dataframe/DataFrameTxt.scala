package com.kevin.scala.dataframe

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * 读取txt文件转成DataFrame形式操作
  */
object DataFrameTxt {

  def main(args: Array[String]): Unit = {
    // 1.创建sparkconf
    val conf = new SparkConf().setAppName("DataFrameTxt").setMaster("local")
    // 2.创建sc
    val sc = new SparkContext(conf)
    // 3.创建sqlcontext
    val sqlContext = new SQLContext(sc)
    val line = sc.textFile("DTSparkSql\\src\\main\\resources\\person.txt")
    import sqlContext.implicits._
    // 4.读取文件用map切分，再用map将数据装成Person类型，toDF转成DataFrame
    line.map(_.split(",")).map(p => new Person(p(0),p(1),p(2).trim.toInt)).toDF.show()
    // 5.关闭sc
    sc.stop()

  }

}
