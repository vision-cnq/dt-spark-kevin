package com.kevin.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 动态统计出现次数最多的单词个数，并过滤掉
  */
object WordFilterDemo {

  def main(args: Array[String]): Unit = {
    val file = "DTSparkCore\\src\\main\\resources\\records.txt"
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("WordFilterDemo").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.textFile读取文件数据,flatMap切分数据,map将单词次数初始值为1返回tuple2格式,sample动态抽样数据作为统计
    val sample = sc.textFile(file).flatMap(_.split("\n")).map(line => (line,1)).sample(true,0.5)
    // 4.reduceByKey将相同的单词次数累加,map将次数作为key,sortByKey降序排序,map重新将单词作为key
    val sortMap = sample.reduceByKey(_+_).map(line => (line._2,line._1)).sortByKey(false).map(value => (value._2,value._1))
    // 5.take获取次数最多的一条数据
    val take:Array[(String,Int)] = sortMap.take(1)
    // 打印最多的那条数据
    println(take(0))
    // 6.filter过滤掉最多的一条数据,foreach遍历,println打印
    sortMap.filter(value => !value._1.equals(take(0)._1)).foreach(println(_))
    // 7.关闭sc
    sc.stop()

  }

}
