package com.kevin.scala.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 对Key做排序,sortByKey()中的参数为false时降序，为true时升序
  */
object SortByKey_RDD {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("SortByKey_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    val file = "DTSparkCore\\src\\main\\resources\\words.txt"
    // 3.textFile读取文件数据,flatMap根据空格切分数据,map将每个Key的初始化为1,reduceByKey将相同的Key的Value值相加,map将将Key，Value的值倒过来，为了对Value的数据做排序
    val rdd = sc.textFile(file).flatMap(_.split(" ")).map(value => (value,1)).reduceByKey(_+_).map(v => (v._2,v._1))
    // 4.sortByKey对Key降序排序,map将Key，Value的值位置再次倒回来,foreach遍历输出
    rdd.sortByKey(false).map(v => (v._2,v._1)).foreach(result => println(result._1+","+result._2))
    // 5.关闭sc
    sc.stop()

  }
}
