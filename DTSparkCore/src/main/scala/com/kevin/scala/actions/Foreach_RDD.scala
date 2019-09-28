package com.kevin.scala.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用spark的foreach函数进行遍历数据
  */
object Foreach_RDD {

  def main(args: Array[String]): Unit = {
    // 创建集合
    val list = List("a", "b", "c", "d", "e")
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("Foreach_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.parallelize将集合转成rdd,foreach遍历,println打印
    sc.parallelize(list).foreach(println(_))
    // 4.关闭sc
    sc.stop()
  }

}
