package com.kevin.scala.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * java的parallelizePairs在scala中没有，所以还是parallelize，
  *     parallelize将Tuple2<K,V>类型的list转为成RDD
  */
object ParallelizePairs_RDD {

  def main(args: Array[String]): Unit = {

    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("ParallelizePairs_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.创建tuple2元组集合
    val list = List((0, "aa"),(1, "a"),(2, "b"),(3, "c"))
    // 4.parallelize将Tuple2类型的list转为成RDD,foreach遍历输出
    sc.parallelize(list).foreach(value => (println(value._1+","+value._2)))
    // 5.关闭sc
    sc.stop()

  }

}
