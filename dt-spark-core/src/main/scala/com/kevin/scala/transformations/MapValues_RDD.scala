package com.kevin.scala.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * mapValues是对<K,V>的V值进行map操作
  */
object MapValues_RDD {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("MapValues_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.parallelize将集合转成rdd,mapValues对key的Value值操作,foreach遍历输出
    sc.parallelize(List(("a",100),("b",200),("c",300))).mapValues(value => value+"~").foreach(value => println("key: "+value._1+" ,value: "+value._2))
    // 4.关闭sc
    sc.stop()
  }
}
