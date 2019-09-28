package com.kevin.scala.transformations

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * mapPartitionsWithIndex与mapPartitions的相似，只是增加了分区的索引
  *   对每个分区的迭代器进行计算，并得到当前分区的索引，返回一个迭代器
  */
object MapPartitionWithIndex_RDD {

  def main(args:Array[String]) : Unit={
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("MapPartitionWithIndex_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.创建数据
    val names = List("java", "scala", ".net","python")
    // 4.parallelize将集合转成rdd,mapPartitionsWithIndex对数据进行分区处理，但只创建与分区数量相同的对象，并得到当前分区索引
    sc.parallelize(names,3).mapPartitionsWithIndex((index,iter)=>{
      val result = new ArrayBuffer[String]()
      while (iter.hasNext) {
        val next = iter.next()
        println("rdd partition index: "+ index + " ,value: "+next)
        result.+=(next)
      }
      println(result.length)
      result.iterator
      // 是否保存分区信息
    },true).collect()  // 由于mapPartitionsWithIndex是转换算子，在没有行动算子的情况下不会执行，所以，创建行动算子
    // 5.关闭sc
    sc.stop()


  }

}
