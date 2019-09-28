package com.kevin.scala.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * ZipWithIndex会将RDD中的元素和这个元素在RDD中的索引好（从0开始），组合成K,V对
  */
object ZipWithIndex_RDD {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("ZipWithIndex_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.parallelize将集合转成rdd,zipWithIndex根据索引组合成tuple2,foreach遍历输出
    sc.parallelize(List("java", "scala", "python")).zipWithIndex().foreach(result => println(result._1+","+result._2))
    // 4.关闭sc
    sc.stop()

  }

}
