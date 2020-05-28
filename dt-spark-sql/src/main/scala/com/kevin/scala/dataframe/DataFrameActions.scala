package com.kevin.scala.dataframe

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext


/**
  *  读取txt文件转为DataFrame表的所有action操作
  */
object DataFrameActions {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("DataFrameActions").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.创建SQLContext对象对sql进行分析处理
    val sqlContext = new SQLContext(sc)
    // 4.导入隐饰操作，否则RDD无法调用toDF方法
    import sqlContext.implicits._
    val file = "DTSparkSql\\src\\main\\resources\\person.txt"
    // 5.textFile读取文件数据,map用逗号切分数据,再map将数据类型转成Person。注：Person需要使用单列对象 case class，才能使用toDF转成DataFrame类型
    val df = sc.textFile(file).map(_.split(",")).map(p => Person(p(0), p(1), p(2).trim.toInt)).toDF()

    // 6.查看所有数据，默认前20行
    df.show()

    // 7.查看前n行数据
    df.show(2)

    // 8.是否最多只显示20个字符，默认为true
    df.show(true)
    df.show(false)

    // 9.显示前n行数据，以及对过长字符串显示格式
    df.show(2,false)

    // 10.打印schema
    df.printSchema()

    // 11.collect将数据转成数组,foreach遍历数据,getAs获取列名为name的数据
    df.collect().foreach(x => println("数据中的name: "+x.getAs("name")))

    // 12.collectAsList将数据转成集合
    println("collectAsList: "+df.collectAsList())

    // 13.describe获取指定字段的统计信息
    df.describe("name").show()

    // 14.first获取第一行记录
    println("获取第一行记录中的name: "+df.first().getAs("name"))

    // 15.head获取第一行记录，head(n)获取前n行记录，下标从1开始
    df.head(2).foreach(x => println("head中name: "+x.getAs("name")))

    // 16.take获取前n行数据，下标从1开始，使用take和takeAsList会将获得到的数据返回到Driver端，所以，使用这两个方法的时候需要少量数据
    df.take(2).foreach(x => println("take中name: "+x.getAs("name")))

    // 17.takeAsList获取前n行记录，以list形式展现
    println("takeAsList: " +df.takeAsList(2))

    // 18.关闭sc
    sc.stop()

  }

}

case class Person(id:String,name: String, age: Int)
