package com.kevin.scala.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 在获取文件数据的时候指定最小分区，并且在遍历的时候使用foreachPartition进行分区输出
  */
object ForeachPartition_RDD {

  def main(args: Array[String]): Unit = {
    val file = "DTSparkCore\\src\\main\\resources\\test.txt"
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("ForeachPartition_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.textFile读取文件数据并设置最小分区为3,foreachPartition分区为3个分区遍历,如果直接使用foreach则默认只使用一个分区
    sc.textFile(file,3).foreachPartition(_.foreach(println(_)))
    // 4.关闭sc
    sc.stop()
  }

}
