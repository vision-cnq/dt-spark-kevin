package com.kevin.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Checkpoint 是为了最大程度保证绝对可靠的复用 RDD 计算数据的 Spark 的高级功能，
  *  通过 Checkpoint 我们通过把数据持久化到 HDFS 上来保证数据的最大程度的安全性
  *    使用场景：多个rdd需要重复被使用的时候，可以将其cache缓存，然后为了防止某些故障导致数据的遗失，使用CheckPoint做为容错
  */
object CheckPointDemo {

  def main(args: Array[String]): Unit = {
    val file = "DTSparkCore\\src\\main\\resources\\records.txt"
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("CheckPointDemo").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.checkpoint持久化的存放路径
    sc.setCheckpointDir("C:\\Users\\caonanqing\\Desktop\\")
    // 4.textFile读取文件数据,flatMap对每行数据初始化次数值为1,以K,V的形式返回,将数据放到缓存
    val rdd = sc.textFile(file).flatMap(value => (List(value,1))).cache()
    // 5.为rdd设置检查点
    rdd.checkpoint()
    // 6.cache和checkpoint都是转换算子，需要执行算子来执行该操作
    rdd.collect()
    println("isCheckPointed: "+rdd.isCheckpointed)
    println("checkpoint: "+rdd.getCheckpointFile)
    // 7.关闭
    sc.stop()
  }

}
