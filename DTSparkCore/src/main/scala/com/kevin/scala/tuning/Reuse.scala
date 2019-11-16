package com.kevin.scala.tuning

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 尽可能复用同一个RDD
  */
object Reuse {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("Reuse").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    val file = "DTSparkCore\\src\\main\\resources\\words.txt"
    // 错误示例
    //    val rdd = sc.textFile(file).flatMap(_.split(" ")).map(value => (value,1))
    //    val rdd1 = sc.textFile(file).flatMap(_.split(" "))
    //    rdd.reduceByKey(_+_)
    //    rdd1.filter(value => !value.contains("java"))
    // 优化一：创建的rdd源只有一个。优化二：复用RDD
    val rdd = sc.textFile(file).flatMap(_.split(" ")).map(value => (value,1))
    // 取key的次数
    rdd.reduceByKey(_+_)
    // 过滤掉java
    rdd.filter(value => !value._1.contains("java"))

    // 关闭sc
    sc.stop()

  }

}
