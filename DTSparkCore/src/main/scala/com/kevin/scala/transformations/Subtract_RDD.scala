package com.kevin.scala.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 取一个RDD与另一个RDD的差集
  */
object Subtract_RDD {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("Subtract_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.分别创建两个相同类型的list转换成rdd
    val rdd1 = sc.parallelize(List("a","b","c"))
    val rdd2 = sc.parallelize(List("a","e","f"))
    // 4.subtract取一个rdd与另一个rdd的差集,foreach遍历输出
    rdd1.subtract(rdd2).foreach(println(_))
    // 5.关闭sc
    sc.stop()



  }

}
