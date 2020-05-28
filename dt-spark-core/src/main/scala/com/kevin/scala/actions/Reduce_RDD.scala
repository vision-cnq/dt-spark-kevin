package com.kevin.scala.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 根据聚合逻辑聚合数据集中的每个元素
  */
object Reduce_RDD {

  def main(args: Array[String]): Unit = {
    // 创建集合
    val list = List(1,2,3,4,5,6)
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("Reduce_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.parallelize将集合转成rdd,reduce将值进行累加
    val reduce = sc.parallelize(list).reduce(_+_)
    // 4.打印
    println(reduce)
    // 5.关闭sc
    sc.stop()
  }
}
