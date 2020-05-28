package com.kevin.scala.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 过滤符合条件的数据，true保留，false过滤
  */
object Filter_RDD {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("Filter_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.创建数据
    val file = "DTSparkCore\\src\\main\\resources\\test2.txt"
    // 4.textFile读取文件,filter过滤包含server的数据,foreach遍历输出
    sc.textFile(file).filter(value => !value.contains("server")).foreach(println(_))
    // 5.关闭sc
    sc.stop()

  }

}
