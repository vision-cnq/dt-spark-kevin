package com.kevin.scala.transformations

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * RePartition重分区，来进行数据紧缩，减少分区数量，将小分区合并为大分区，从而提高效率
  */
object RePartition_RDD {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("RePartition_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.创建集合
    val list = List("love1","love2","love3", "love4","love5","love6", "love7","love8","love9", "love10","love11","love12")
    // 4.parallelize集合转成rdd并设置分区,然后对数据进行分区处理，但只创建与分区数量相同的对象，并得到当前分区索引
    val rdd = sc.parallelize(list, 3).mapPartitionsWithIndex((index, iter) => {
      val result = new ArrayBuffer[String]()
      while (iter.hasNext) {
        result.+=(iter.next())
      }
      result.iterator
    }, true)
    println("repartition之前的分区数: "+rdd.partitions.size)

    // 5.重新划分分区
    val repartition = rdd.repartition(2)

    // 6.对数据进行分区处理，但只创建与分区数量相同的对象，并得到当前分区索引
    val result = repartition.mapPartitionsWithIndex((index,iter)=>{
      val result = new ArrayBuffer[String]()
      while (iter.hasNext) {
        result.+=(iter.next())
      }
      result.iterator
    },true)

    println("repartition之后的分区数: "+result.partitions.size)
    // 7.关闭sc
    sc.stop()

  }

}
