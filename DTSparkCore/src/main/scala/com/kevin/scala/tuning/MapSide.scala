package com.kevin.scala.tuning

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用map-side预聚合的shuffle操作
  */
object MapSide {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("MapSide").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.创建tuple2类型的集合数据
    val list = List("a","b","b","c","c","d","d")
    // 4.parallelize将集合转成rdd,map将单词次数初始值设为1
    val rdd = sc.parallelize(list).map(word => (word,1))
    // 未优化: groupByKey将相同的key分组并将其Value相加,foreach遍历数据
    // rdd.groupByKey().map(g => (g._1,g._2.sum)).foreach(value => (println("groupByKey: "+value._1+","+value._2)))
    // 优化: 使用reduceByKey代替groupByKey先做预聚合
    // reduceByKey将相同的key的value次数相加,foreach遍历输出
    rdd.reduceByKey(_+_).foreach(value => (println("reduceByKey: "+value._1+","+value._2)))
    // 5.关闭sc
    sc.stop()
  }

}
