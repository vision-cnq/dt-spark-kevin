package com.kevin.scala.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
  * first是获取第一个数据
  */
object First_RDD {

  def main(args: Array[String]): Unit = {
    // 创建集合
    val list = List("a", "b", "c", "d", "e")
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("First_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.parallelize将集合转成rdd,first取第一个数据
    val first = sc.parallelize(list).first()
    // 4.关闭sc
    println(first)
    sc.stop()
  }

}
