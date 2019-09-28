package com.kevin.scala.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * java的flatMapToPair在scala中没有，所以还是flatMap，
  *       输入一条数据，输出多条数据。一对多：类型是list的Tuple2<K,V>
  */
object FlatMapToPair_RDD {

  def main(args: Array[String]): Unit = {

    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("FlatMapToPair_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.创建数据
    val file = "DTSparkCore\\src\\main\\resources\\words.txt"
    // 4.textFile读取文件数据,flatMap像Hadoop的Map一样返回K,V类型的list,foreach遍历输出
    sc.textFile(file).flatMap(value => (List((value,1)))).foreach(value => (println("key: " + value._1 +" ,value: "+ value._2)))
    // 5.关闭sc
    sc.stop()
  }

}
