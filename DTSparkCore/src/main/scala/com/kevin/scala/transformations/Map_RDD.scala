package com.kevin.scala.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * map处理传入的每个元素，输入一条，输出一条。一对一：类型是对象
  */
object Map_RDD {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("Map_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.创建数据
    val list = List("Hello World", "Word Count")
    // 3.parallelize将集合转成rdd,map切分数据,collect转成集合,foreach遍历输出
    sc.parallelize(list).map(_.split(" ")).collect().foreach(value => (value.foreach(println(_))))
    // 4.关闭sc
    sc.stop()


  }

}
