package com.kevin.scala.transformations

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * combineByKey是针对不同partition进行操作的。第一个参数用于数据初始化，第二个参数是同个partition内combine操作函数，
  *   第三个参数是在所有partition都combine完后，对所有临时结果进行combine操作的函数。
  */
object CombineByKey_RDD {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("CombineByKey_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 创建数据
    val list = List(("a",1),("b",2),("b",3),("a",4),("b",5),("c",6))
    // 3.将集合转成rdd并分区
    val parallelize = sc.parallelize(list, 2)
    // 4.对数据进行分区处理，但只创建与分区数量相同的对象，并得到当前分区索引
    parallelize.mapPartitionsWithIndex((index, iter)=>{
      val result = new ArrayBuffer[(String,Int)]()
      while (iter.hasNext) {
        val next = iter.next()
        println("rdd1 partition index: " + index + " ,value: " + next)
        result.+=(next)
      }
      println(result.length)
      result.iterator
    },true).collect()

    // 5.combineByKey是针对不同partition进行操作的。
    // 参数1:用于数据初始化，参数2:是同个partition内combine操作函数，参数3:是在所有partition都combine完后，对所有临时结果进行combine操作的函数。
    parallelize.combineByKey(
      // 初始端,把当前分区的第一个值当做v1，这里给每个partition相当于初始化值，如果当前partition的key已初始化值则执行第二个函数
      (one :Int) => {
        println("one value: "+one)
        one
      }
      // Combiner聚合逻辑,合并在同一个partition中的值
      ,(v1 :Int,v2 :Int) => {
        println("combiner value: "+(v1+v2))
        v1+v2
        }
      // reduce聚合逻辑,合并不同的partition中的值，该函数在前面两个函数已经执行完之后才会执行
      ,(v1 :Int,v2 :Int) => {
        println("reduce value: " + (v1+v2))
        v1+v2
        }
    // 遍历数据
    ).foreach(value=>println("result: " + value))
    // 6.关闭sc
    sc.stop()
  }

}
