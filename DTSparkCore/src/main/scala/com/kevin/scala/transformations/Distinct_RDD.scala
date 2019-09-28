package com.kevin.scala.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 对原RDD进行去重做操作，返回不重复的成员
  */
object Distinct_RDD {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("Distinct_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.创建集合数据
    val list = List("Java", "scala", "Python", "Java")
    // 4.parallelize将集合转成rdd,distinct去重,foreach遍历打印
    sc.parallelize(list).distinct().foreach(println(_))
    // 5.关闭sc
    sc.stop()

  }

}
