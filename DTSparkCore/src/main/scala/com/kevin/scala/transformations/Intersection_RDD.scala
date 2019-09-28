package com.kevin.scala.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 取两个RDD之间的交集
  */
object Intersection_RDD {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("Intersection_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.parallelize将集合转成rdd
    val rdd1 = sc.parallelize(List("a", "b", "c"))
    val rdd2 = sc.parallelize(List("a", "e", "f"))
    // 4.取rdd1和rdd2之间的交集,foreach遍历
    rdd1.intersection(rdd2).foreach(value => (println("两个rdd之间的交集: " + value)))
    // 5.关闭sc
    sc.stop()

  }

}
