package com.kevin.scala.tuning

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 避免创建重复的RDD
  */
object AvoidRepetition {

  def main(args: Array[String]): Unit = {
    val file = "DTSparkCore\\src\\main\\resources\\records.txt"
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("AvoidRepetition").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.不应该创建多个相同数据源RDD
    //    val rdd1 = sc.textFile(file)
    //    val rdd2 = sc.textFile(file)
    //    val tuples = rdd1.map(_.split(" ")).collect()
    //    val array = rdd2.flatMap(_.split(" "))
    //    tuples.foreach(value => (value.foreach(println(_))))
    //    array.foreach(println(_))
    // 优化一：使用同一个数据源RDD，优化二：使用cache和checkpoint做持久化
    val rdd1 = sc.textFile(file).cache()
    sc.setCheckpointDir("C:\\Users\\caonanqing\\Desktop\\persistence")
    rdd1.checkpoint()
    val tuples = rdd1.map(_.split(" ")).collect()
    val array = rdd1.flatMap(_.split(" "))

    tuples.foreach(value => (value.foreach(println(_))))
    array.foreach(println(_))

    // 关闭
    sc.stop()

  }

}
