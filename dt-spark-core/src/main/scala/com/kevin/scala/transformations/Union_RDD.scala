package com.kevin.scala.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  *  合并两个RDD，不去重，要求两个RDD钟的元素类型一致，逻辑上合并
  */
object Union_RDD {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("Union_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.分别创建两个相同类型的list转换成rdd
    val rdd1 = sc.parallelize(List("KEVIN", "COCO"))
    val rdd2 = sc.parallelize(List("boy", "grid"))
    // 4.union合并两个RDD,foreach遍历输出
    rdd1.union(rdd2).foreach(println(_))
    // 5.关闭sc
    sc.stop()
  }

}
