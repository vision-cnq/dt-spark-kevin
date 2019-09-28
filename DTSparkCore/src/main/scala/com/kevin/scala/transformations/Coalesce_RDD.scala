package com.kevin.scala.transformations

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * coalesce减少分区
  *   第二个参数是减少分区的过程中是否产生shuffle，true是产生shuffle，false是不产生shuffle，默认是false.
  *   如果coalesce的分区数比原来的分区数还多，第二个参数设置false，即不产生shuffle,不会起作用。
  *   如果第二个参数设置成true则效果和repartition一样，即coalesce(numPartitions,true) = repartition(numPartitions)
  *   可以在Filter后进行Coalesce重分区来减少数据倾斜。
  */
object Coalesce_RDD {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("Coalesce_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.创建list
    val list = List("love1","love2","love3", "love4","love5","love6", "love7","love8","love9", "love10","love11","love12")
    // 4.parallelize将集合转成rdd并设置分区,然后对数据进行分区处理，但只创建与分区数量相同的对象，并得到当前分区索引
    val rdd = sc.parallelize(list, 3).mapPartitionsWithIndex((index, iter) => {
      val result = new ArrayBuffer[String]()
      while (iter.hasNext) {
        val next = iter.next()
        println("partition index: " + index + " ,value: " + next)
        result.+=(next)
      }
      println(result.length)
      result.iterator
    }, true)
    println("coalesce之前分区数: " + rdd.partitions.size)

    // 设置分区数大于原RDD的分区数且不产生shuffle，不起作用
    // rdd.coalesce(4,false)
    // //设置分区数大于原RDD的分区数且产生shuffle，相当于repartition
    // rdd.coalesce(4,true)

    // 5.coalesce减少分区,true为产生shuffle,flase为不产生shuffle
    val coalesce = rdd.coalesce(2,true)
    // 6.重分区，对数据进行分区处理，但只创建与分区数量相同的对象，并得到当前分区索引
    val result = coalesce.mapPartitionsWithIndex((index, iter) => {
      val result = new ArrayBuffer[String]()
      while (iter.hasNext) {
        val next = iter.next()
        println("coalesce rdd the partition index: " + index + " ,value: " + next)
        result.+=(next)
      }
      println(result.length)
      result.iterator
    }, true)

    println("coalesce之后分区数: " + result.partitions.size)
    // 7.关闭sc
    sc.stop()

  }

}
