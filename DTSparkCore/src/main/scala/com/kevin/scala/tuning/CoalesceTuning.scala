package com.kevin.scala.tuning

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用filter之后进行coalesce操作
  */
object CoalesceTuning {

  def main(args: Array[String]): Unit = {
    val file = "DTSparkCore\\src\\main\\resources\\test2.txt"
    // 创建SparkConf
    val conf = new SparkConf().setAppName("CoalesceTuning").setMaster("local")
    // 创建SparkContext
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(file,5)
    println("rdd: "+rdd.getNumPartitions)
    println("原数据量: "+rdd.count())

    // 过滤数据
    val rdd1 = rdd.filter(value => !value.contains("server"))

    // coalesce减少分区,true为产生shuffle,flase为不产生shuffle
    val coalesce = rdd1.coalesce(3,true)
    println("coalesce: "+coalesce.getNumPartitions)
    println("过滤后: "+coalesce.count())
    // 关闭sc
    sc.stop()
  }

}
