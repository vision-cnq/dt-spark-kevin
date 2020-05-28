package com.kevin.scala.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Sample随机抽样：
  *     withReplacement：false(不会相同)、true(可能会相同)，
  *     fraction：抽样的比例，
  *     seed：指定随机数生成器种子，种子不同，则序列才会不同
  */
object Sample_RDD {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("Sample_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    // 3.parallelize将集合数据转成rdd
    val rdd = sc.parallelize(list)
    // 4.随机抽样，为false时抽出就不放回，下次抽到的数据不可能会相同
    rdd.sample(false,0.6,System.currentTimeMillis()).foreach(value => println("sample false: "+value))
    // 5.随机抽样，为true时抽出还会放回，下次抽到数据还可能会相同
    rdd.sample(true,0.6,System.currentTimeMillis()).foreach(value => println("sample true: "+value))
    // 6.关闭sc
    sc.stop()

  }

}
