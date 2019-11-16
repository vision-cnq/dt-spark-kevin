package com.kevin.scala.tuning

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * 使用mapPartitions替代普通map
  */
object MapTuning {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("MapTuning").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    val file = "DTSparkCore\\src\\main\\resources\\records.txt"
    // 未优化：使用map，一次函数只处理一条数据
    // sc.textFile(file).map(_.split(" ")).collect().foreach(value => (value.foreach(println(_))))
    // 优化：改为mapPartitions并设置分区，每次函数处理一个partition的数据
    sc.textFile(file,3).mapPartitions(iter => {
      val result = new ArrayBuffer[String]()
      while (iter.hasNext) {
        for(value <- iter.next().split(" ")){
          result.+=(value)
        }
      }
      result.iterator
      //    例如
      //      con = getConnect() //获取数据库连接
      //      iter.foreach(iter=>{
      //        con.insert(data) //循环插入数据
      //      })
      //      con.commit() //提交数据库事务
      //      con.close() //关闭数据库连接
    }).collect().foreach(println(_))

    // 4.关闭sc
    sc.stop()

  }

}
