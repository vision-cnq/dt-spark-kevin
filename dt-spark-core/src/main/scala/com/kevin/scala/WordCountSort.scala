package com.kevin.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 单词计数，并且排序
  */
object WordCountSort {

  def main(args: Array[String]): Unit = {
    val file = "DTSparkCore\\src\\main\\resources\\test.txt"
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("WordCountSort").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.textFile读取文件数据,flatMap以空格切分数据,map将所有的单词次数初始值设为1，reduceByKey将所有相同单词的次数相加,map为了排序将次数作为key单词作为value
    val map = sc.textFile(file).flatMap(_.split(" ")).map(word => (word,1)).reduceByKey(_+_).map(value => (value._2,value._1))
    // 4.sortByKey降序排序,map将已经排序的数据键值对转回来,foreach遍历
    map.sortByKey(false).map(value => (value._2,value._1)).foreach(value => (println("Word: "+value._1+" ,count: "+value._2)))
    // 5.关闭
    sc.stop()

  }

}
