package com.kevin.scala.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 相同的Key分组并将其Value值聚合在一起
  */
object GroupByKey_RDD {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("GroupByKey_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.创建tuple2类型的集合数据
    val list = List(("a",100),("b",200),("b",100),("c",100),("c",300),("d",100),("d",200))
    // 4.parallelize将集合转成rdd,groupByKey将相同的key分组并将其Value聚合在一起,foreach遍历数据
    sc.parallelize(list).groupByKey().foreach(println(_))
    // 5.关闭sc
    sc.stop()
  }

}
