package com.kevin.scala.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * 根据数据集每个元素相同的内容来计数，返回相同内容元素对应的条数
  */
object CountByValue_RDD {

  def main(args: Array[String]): Unit = {
    // 创建一个Tuple2元组集合
    val list = List((1, "a"),(2, "b"),(3, "c"),(4, "d"),(4, "e"))
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("CountByValue_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.parallelize将集合转成rdd,countByKey统计相同的key的value次数返回(key,value),foreach遍历,println打印
    sc.parallelize(list).countByValue().foreach(println(_))
    // 4.关闭sc
    sc.stop()
  }

}
