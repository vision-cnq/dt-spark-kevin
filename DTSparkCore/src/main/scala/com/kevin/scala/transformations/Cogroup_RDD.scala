package com.kevin.scala.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 当调用类型（K，V）和（K，W）的数据集时，
  *   返回一个数据集（K，（Iterable <V>，Iterable <W>））元组。此操作也被称做groupWith。
  */
object Cogroup_RDD {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("Cogroup_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.创建student数据
    val studentList = List(("1","zhangsan"),("2","lisi"),("2","wangwu"),("3","maliu"))
    // 4.创建score数据
    val scoreList = List(("1","100"),("2","90"),("3","80"),("1","97"),("2","60"),("3","50"))
    // 5.将student数据转为rdd
    val studentRDD = sc.parallelize(studentList)
    // 6.将score数据转为rdd
    val scoreRDD = sc.parallelize(scoreList)
    // 7.cogroup将所有相同的key的value值聚合在一起，主动cogroup的是数组1，被动的是数组2
    val cogroup = studentRDD.cogroup(scoreRDD)
    // 8.遍历
    cogroup.foreach(println(_))
    // 9.关闭sc
    sc.stop()



  }

}
