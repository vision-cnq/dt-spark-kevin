package com.kevin.scala.tuning

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将重复使用的RDD进行持久化
  */
object Persistence {

  def main(args: Array[String]): Unit = {
    val file = "DTSparkCore\\src\\main\\resources\\records.txt"
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("Persistence").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.checkpoint持久化的存放路径
    sc.setCheckpointDir("C:\\Users\\caonanqing\\Desktop\\persistence")
    // 4.textFile读取文件数据,flatMap对每行数据初始化次数值为1,以K,V的形式返回,将数据放到缓存
    val rdd = sc.textFile(file).flatMap(value => (List(value,1))).cache()
    // 5.将rdd存储起来
    rdd.checkpoint()
    // 6.cache和checkpoint都是转换算子，需要执行算子来执行该操作
    rdd.collect()
    println("isCheckPointed: "+rdd.isCheckpointed)
    println("checkpoint: "+rdd.getCheckpointFile)
    // 7.关闭
    sc.stop()

  }

}
