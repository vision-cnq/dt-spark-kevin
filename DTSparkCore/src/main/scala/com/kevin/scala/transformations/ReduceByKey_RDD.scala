package com.kevin.scala.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 相同的Key进行Reduce操作
  */
object ReduceByKey_RDD {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("ReduceByKey_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    val file = "DTSparkCore\\src\\main\\resources\\words.txt"
    // 3.textFile读取文件数据,flatMap用空格切分数据,map将次数初始值为1,reduceByKey将相同的key的value次数相加,foreach遍历输出
    sc.textFile(file).flatMap(_.split(" ")).map(value => (value,1)).reduceByKey(_+_).foreach(value => (println(value._1+","+value._2)))
    // 4.关闭sc
    sc.stop()

  }

}
