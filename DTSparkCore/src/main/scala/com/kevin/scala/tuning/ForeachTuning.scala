package com.kevin.scala.tuning

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用foreachPartition替代foreach
  */
object ForeachTuning {

  def main(args: Array[String]): Unit = {
    val file = "DTSparkCore\\src\\main\\resources\\test.txt"
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("ForeachTuning").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 未优化：使用foreach一次函数遍历一个数据
    // sc.textFile(file).foreach(println(_))
    // 优化：foreachPartition分区为3个分区遍历,一次函数变量一个partition的数据
    sc.textFile(file,3).foreachPartition(_.foreach(println(_)))
    // 4.关闭sc
    sc.stop()
  }

}
