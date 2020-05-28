package com.kevin.scala.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * java的mapToPair在scala中没有，所以还是map，
  *     map处理传入的每个元素，输入一条，输出一条。一对一：类型是Tuple2<K,V>
  */
object MapToPair_RDD {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("MapToPair_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    val file = "DTSparkCore\\src\\main\\resources\\test2.txt"
    // 3.textFile读取文件数据,flatMap根据空格切分数据,map统计切分的数据，输入一条输出一条,foreach遍历输出
    sc.textFile(file).flatMap(value => value.split(" ")).map(value => (value,1)).foreach(println(_))
    // 4.关闭sc
    sc.stop()
  }
}
