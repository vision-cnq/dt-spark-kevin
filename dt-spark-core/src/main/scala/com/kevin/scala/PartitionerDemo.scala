package com.kevin.scala

import org.apache.spark._

import scala.collection.mutable.ArrayBuffer

/**
  * 自定义分区器
  */
object PartitionerDemo {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("PartitionerDemo").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 创建元组集合
    val list = List((1,"a"),(2,"b"),(3,"c"),(4,"d"),(5,"e"),(6,"f"))
    // 3.转为rdd并设置分区为2
    val rdd = sc.parallelize(list,2)
    // 4.自定义分区前
    rdd.mapPartitionsWithIndex((index,iter) => {
      val result = new ArrayBuffer[(Int,String)]()
      while (iter.hasNext) {
        val next = iter.next()
        println("自定义分区前 partitionID = "+index+" , value = "+next)
        result.+=(next)
      }
      println(result.length)
      result.iterator
    }, true).collect()
    println("----------")
    // 5.自定义分区规则
    val partitionBy = rdd.partitionBy(new MyPartitioner(4))

    // 6.自定义分区后
    partitionBy.mapPartitionsWithIndex((index,iter) => {
      val result = new ArrayBuffer[(Int,String)]()
      while (iter.hasNext) {
        val next = iter.next()
        println("自定义分区后 partitionID = "+index+" , value = "+next)
        result.+=(next)
      }
      println(result.length)
      result.iterator
    },true).collect()
    // 7.关闭sc
    sc.stop()

  }
}

/**
  * 自定义分区类，需继承Partitioner类
  * @param num
  */
class MyPartitioner(num: Int) extends Partitioner {
  // 覆盖分区数
  override def numPartitions: Int = num
  // 覆盖分区号获取函数
  override def getPartition(key: Any): Int = {
    if(key.toString.toInt % 2 == 0) {
      0
    } else {
      1
    }
  }
}
