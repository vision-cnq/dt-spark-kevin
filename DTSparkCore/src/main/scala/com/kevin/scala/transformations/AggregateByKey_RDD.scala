package com.kevin.scala.transformations

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * 对PairRDD中相同的Key值进行聚合操作，在聚合过程中同样使用了一个中立的初始值。和aggregate函数类似，
  *   aggregateByKey返回值的类型不需要和RDD中value的类型一致。因为aggregateByKey是对相同Key中的值进行聚合操作，
  *   所以aggregateByKey函数最终返回的类型还是PairRDD，对应的结果是Key和聚合后的值，而aggregate函数直接返回的是非RDD的结果
  */
object AggregateByKey_RDD {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("AggregateByKey_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.创建tuple2集合数据
    val list = List((1, 1),(2, 2),(1, 3),(2, 4),(3, 5),(3, 6))
    // 4.集合转成rdd设置分区为2
    val rdd = sc.parallelize(list,2)
    // 5.对分区的数据进行处理，但只创建与分区数量相同的对象，并得到当前分区索引
    rdd.mapPartitionsWithIndex((index,iter) => {
      val result = new ArrayBuffer[(Int,Int)]()
      while (iter.hasNext) {
        val next = iter.next()
        println("partition index: " + index + " ,value: " + next)
        result.+=(next)
      }
      println(result.length)
      result.iterator
    },true).collect()
    println("----------")

    // 6.设置中立的值并参与计算，先对相同分区的数据进行处理，再对不同分区相同key的数据进行处理，最后转成集合再遍历
    rdd.aggregateByKey(80)(
      (v1,v2)=>{
        // 合并在同一个partition中的值，v1的数据类型为zeroValue的数据类型，v2的数据类型为原value的数据类型，对同一个partition中的数据进行处理
        println("Seq: "+v1+"\t"+v2)
        v1+v2
    },(v1,v2)=>{
        // 合并不同partition中的值，v1，v2的数据类型为zeroValue的数据类型，对不同partition中相同的key进行处理
        println("Comb: "+v1+"\t"+v2)
        v1+v2
    }).collect().foreach(value => (println("key: "+value._1+"value: "+value._2)))
    // 7.关闭sc
    sc.stop()

  }

}
