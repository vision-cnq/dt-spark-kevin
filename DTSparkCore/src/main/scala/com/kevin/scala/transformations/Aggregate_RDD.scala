package com.kevin.scala.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将每个分区里面的元素进行聚合，然后用combine函数将每个分区的结果和初始值(zeroValue)进行combine操作。
  * 这个函数最终返回的类型不需要和RDD中元素类型一致。
  */
object Aggregate_RDD {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("Aggregate_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 创建list数据
    val list = List(9, 5, 7, 4, 4, 2, 2)
    // 3.parallelize将集合转成rdd并设置分区为3,aggregate设置中立值作为计算，先对分区的数据聚合，再将每个分区的结果和初始值进行Comb
    val aggregate = sc.parallelize(list,3).aggregate(2)(
      (v1,v2) => {
        // 对同个partition的值进行合并,从设置的中立值开始加第一个值
        println("Sequ: " + v1+"\t"+v2)
        v1+v2
    },(v1,v2) => {
        // 将每个partition的结果值进行合并,从设置的中立值开始加第一个结果值
        println("Comb: " + v1+"\t"+v2)
        v1+v2
    })
    println("aggregate处理的最终结果: "+aggregate)
    sc.stop()
  }

}
