package com.kevin.scala.transformations

import java.util

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * 对rdd中的每个分区的迭代器进行操作。返回一个可迭代的对象
  *   如果是普通的map，比如一个partition中有1万条数据。ok，那么你的function要执行和计算1万次。
  *   使用MapPartitions操作之后，一个task仅仅会执行一次function，function一次接收所有的partition数据。
  *   只要执行一次就可以了，性能比较高。如果在map过程中需要频繁创建额外的对象
  *   (例如将rdd中的数据通过jdbc写入数据库,map需要为每个元素创建一个链接而mapPartition
  *   为每个partition创建一个链接),则mapPartitions效率比map高的多。
  *   SparkSql或DataFrame默认会对程序进行mapPartition的优化。
  */
object MapPartitions_RDD {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("MapPartitions_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    val file = "DTSparkCore\\src\\main\\resources\\records.txt"
    // 3.textFile读取文件数据并设置分区,mapPartitions对数据进行分区处理，但只创建与分区数量相同的对象
    sc.textFile(file,3).mapPartitions(iter => {
      val result = new ArrayBuffer[String]()
      while (iter.hasNext) {
        result.+=(iter.next())
      }
      println(result.length)
      result.iterator
//    例如
//      con = getConnect() //获取数据库连接
//      iter.foreach(iter=>{
//        con.insert(data) //循环插入数据
//      })
//      con.commit() //提交数据库事务
//      con.close() //关闭数据库连接
    }).collect()  // 由于mapPartitions是转换算子，在没有行动算子的情况下不会执行，所以，创建行动算子

    // 4.关闭sc
    sc.stop()

  }

}
