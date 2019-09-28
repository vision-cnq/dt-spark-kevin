package com.kevin.scala.secondsort

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 二次排序
  */
object SecondarySort {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("SecondarySort").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    val file = "DTSparkCore\\src\\main\\resources\\secondSort.txt"
    // 3.textFile读取文件数据,map将数据封装成SecondSortKey类型,sortByKey重写排序方法,使用自定义的排序方法,foreach遍历输出
    sc.textFile(file).map(line => {
      val first = line.split(" ")(0).toInt
      val second = line.split(" ")(1).toInt
      (new SecondSortKey(first,second),line)
    }).sortByKey(false).foreach(value => println(value._2))
    // 4.关闭sc
    sc.stop()

  }

}
