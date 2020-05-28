package com.kevin.scala.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Join连接：返回两边都拥有的值
  * leftOuterJoin：返回左边拥有的值，若右边没有则返回空
  * rightOuterJoin：返回右边拥有的值，若左边没有则返回空
  * fullOuterJoin：返回所有的值，左边有，右边就算没有也返回空。右边有，左边就算没有也返回空
  */
object Join_RDD {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("Join_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.parallelize将集合转成rdd
    val rdd1 = sc.parallelize(List((0, "aa"),(1,"a"),(2,"b"),(3,"c")))
    val rdd2 = sc.parallelize(List((1, 100),(2, 200),(3, 300),(4, 400)))
    // 4.join,将K相同的V值连接在一起，返回两边都拥有的值
    rdd1.join(rdd2).foreach(value => (println("join: "+ value)))
    // rightOuterJoin返回右边拥有的值，若左边没有则返回空
    // rdd1.rightOuterJoin(rdd2).foreach(value => (println("rightOuterJoin: "+ value)))
    // leftOuterJoin返回左边拥有的值，若右边没有则返回空
    // rdd1.leftOuterJoin(rdd2).foreach(value => (println("leftOuterJoin: "+ value)))
    // fullOuterJoin返回所有的值，左边有，右边就算没有也返回空。右边有，左边就算没有也返回空
    // rdd1.fullOuterJoin(rdd2).foreach(value => (println("fullOuterJoin: "+ value)))

    // 5.关闭sc
    sc.stop()
  }
}
