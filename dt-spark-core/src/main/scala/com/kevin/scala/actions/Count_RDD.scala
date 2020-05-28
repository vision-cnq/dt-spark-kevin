package com.kevin.scala.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 返回结果集中的行数
  */
object Count_RDD {

  def main(args: Array[String]):Unit = {

    val file = "DTSparkCore\\src\\main\\resources\\test.txt"
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("Count_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.统计数据记录数
    val count = sc.textFile(file).count()
    // 4.打印
    println(count)
    // 5.关闭sc
    sc.stop()

  }

}
