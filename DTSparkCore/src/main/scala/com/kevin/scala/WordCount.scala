package com.kevin.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 单词计数
  */
object WordCount {

    def main(args: Array[String]): Unit = {

        val file = "DTSparkCore\\src\\main\\resources\\test.txt"
        // 1.创建SparkConf，设置作业名称，设置模式为单机模式
        val conf = new SparkConf().setAppName("WordCount").setMaster("local")
        // 2.基于SparkConf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        val sc = new SparkContext(conf)
        // 3.textFile读取文件，flatMap将文件以空格形式切分，map将所有的单词次数初始值为1，reduceByKey将相同的单词的次数相加，collect转为集合，foreach遍历输出
        sc.textFile(file).flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).collect().foreach(println(_))
        // 4.关闭SparkContext
        sc.stop()
    }

}
