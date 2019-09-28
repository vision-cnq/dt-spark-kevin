package com.kevin.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 累加器在Driver端定义赋初始值和读取，在Executor端累加。
  */
object AccumulatorDemo {

  def main(args: Array[String]): Unit = {

    val file = "DTSparkCore\\src\\main\\resources\\records.txt"
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("AccumulatorDemo").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.设置累加器初始值为0
    val accumulator = sc.accumulator(0)
    // 4.textFile读取文件数据,foreach遍历,每遍历一次累加器就加1
    sc.textFile(file).foreach(_ => (accumulator.add(1)))
    // 5.打印累加器的结果
    println(accumulator.value)
    // 6.关闭sc
    sc.stop()
  }

}
