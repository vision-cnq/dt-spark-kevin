package com.kevin.scala.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * zip将两个非KV格式的RDD，通过一一对应的关系压缩成KV格式的RDD
  *    要求：分区数和分区中的元素个数相等
  */
object Zip_RDD {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("Zip_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.分别创建两个相同类型的list转换成rdd
    val rdd1 = sc.parallelize(List("java", "scala", "python"))
    val rdd2 = sc.parallelize(List(100,200,300))
    // 4.zip将值连接构成元祖，必须两个rdd的元素数量，partition数量一致,foreach遍历输出
    rdd1.zip(rdd2).foreach(result => (println(result._1+","+result._2)))
    // 5.关闭sc
    sc.stop()
  }

}
