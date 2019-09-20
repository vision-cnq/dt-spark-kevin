package com.kevin.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 单词计数
  */
object WordCount {

    def main(args: Array[String]): Unit = {

        val file = "DTSparkCore\\src\\main\\resources\\test.txt"
        val conf = new SparkConf().setAppName("WordCountDemo").setMaster("local")
        val sc = new SparkContext(conf)
        sc.textFile(file).flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).collect().foreach(println(_))
        sc.stop()
    }

}
