package com.kevin.scala.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
  *   Collect将计算的RDD结果回收到Driver端，转为List集合。适用于小量数据
  */
object Collect_RDD {

  def main(args: Array[String]): Unit = {

    val file = "DTSparkCore\\src\\main\\resources\\test.txt"
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("Collect_RDD").setMaster("local")

    // 2.创建SparkContext
    val sc = new SparkContext(conf)

    // 3.textFile获取文件数据,filter过滤掉kill,collect转成集合,foreach遍历,println打印
    sc.textFile(file).filter(!_.contains("kill")).collect().foreach(println(_))
    // 4.关闭sc
    sc.stop()

  }

}
