package com.kevin.scala.tuning

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * 使用repartitionAndSortWithinPartition替代reparation与sort类操作
  */
object RepartitionSortTuning {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("RepartitionSortTuning").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    val list = List((1, 1), (2, 2), (1, 3), (5, 4), (3, 5), (7, 6))
    // 3.parallelize将集合转成rdd,repartitionAndSortWithinPartitions先重新分区，再对其进行排序
    val rdd = sc.parallelize(list,5)
    // 未优化：使用repartition+Sort，先shuffle分区，然后在排序
//    val repartition = rdd.repartition(3)
//    repartition.sortByKey(true).map(v => (v._2,v._1)).foreach(result => println(result._1+","+result._2))

    // 优化：使用repartitionAndSortWithinPartitions替代repartition+Sort，一边进行重分区的shuffle操作，一边进行排序
    val rdd1 = rdd.repartitionAndSortWithinPartitions(new HashPartitioner(3))
    println("rdd1 partitions size: " + rdd1.partitions.length)
    // 4.对数据进行分区处理，但只创建与分区数量相同的对象，并得到当前分区索引
    rdd1.mapPartitionsWithIndex((index,iter) => {
      while (iter.hasNext) {
        println("rdd1 partition index: " + index + " ,value: " + iter.next())
      }
      iter
    },true).count()
    // 5.关闭sc
    sc.stop()
  }

}
