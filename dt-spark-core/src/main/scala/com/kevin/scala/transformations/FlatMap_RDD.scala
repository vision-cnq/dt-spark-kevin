package com.kevin.scala.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * flatMap输入一条数据，输出多条数据。一对多：类型是list对象
  */
object FlatMap_RDD {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("FlatMap_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.创建数据
    val file = "DTSparkCore\\src\\main\\resources\\test2.txt"
    // 4.textFile读取文件数据,flatMap切分数据,foreach遍历输出
    sc.textFile(file).flatMap(_.split(" ")).foreach(println(_))
    // 5.关闭sc
    sc.stop()

  }


}
