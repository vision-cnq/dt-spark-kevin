package com.kevin.scala.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 作用到K,V格式的RDD，根据Key计数相同Key的数据集元素，返回一个Map<Integer, Object>
  */
object CountByKey_RDD {

  def main(args:Array[String]):Unit={
    // 创建一个Tuple2元组集合
    val list = List((1, "a"),(2, "b"),(3, "c"),(4, "d"),(4, "e"))
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("CountByKey_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.parallelize将集合转成rdd,countByKey统计相同的key的value次数返回key,foreach遍历,println打印
    sc.parallelize(list).countByKey().foreach(println(_))
    // 4.关闭sc
    sc.stop()
  }
}
