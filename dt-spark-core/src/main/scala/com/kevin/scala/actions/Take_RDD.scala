package com.kevin.scala.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
  * take是获取指定的数据
  */
object Take_RDD {

  def main(args: Array[String]): Unit = {
    val file = "DTSparkCore\\src\\main\\resources\\test.txt"
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("Take_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.textFile读取文件数据,take获取前两行数据,foreach遍历,println打印
    sc.textFile(file).take(2).foreach(println(_))
    // 4.关闭sc
    sc.stop()

  }

}
