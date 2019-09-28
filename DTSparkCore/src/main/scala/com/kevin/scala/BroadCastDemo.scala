package com.kevin.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 广播变量
  *   1.不能将一个RDD使用广播变量广播出去，因为RDD是不存数据的，可以将RDD的结果广播出去。
  *   2.广播变量只能在Driver端定义，不能在Executor端定义。
  *   3.在Driver端可以修改广播变量的值，在Executor端不能修改广播变量的值。
  */
object BroadCastDemo {

  def main(args: Array[String]): Unit = {
    val file = "DTSparkCore\\src\\main\\resources\\records.txt"
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("BroadCastDemo").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.广播变量将list广播出去
    val broadcast = sc.broadcast(List("hello java"))
    // 4.textFile读取文件数据,filter过滤掉非广播数据,foreach遍历
    sc.textFile(file).filter(value => (broadcast.value.contains(value))).foreach(println(_))
    // 5.关闭sc
    sc.stop()

  }

}
