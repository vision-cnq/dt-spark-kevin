package com.kevin.scala.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * parallelize将list数据转为RDD
  */
object Parallelize_RDD {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("Parallelize_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.创建集合数据
    val list = List("Java", "scala", "Python", "Java")
    // 4.parallelize将集合转成rdd,foreach遍历输出
    sc.parallelize(list).foreach(println(_))
    // 5.关闭sc
    sc.stop()

  }

}
