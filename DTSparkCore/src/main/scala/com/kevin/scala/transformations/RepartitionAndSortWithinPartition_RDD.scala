package com.kevin.scala.transformations

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * 如果需要在repartition重分区之后，还要进行排序则推荐使用该算子，
  *   效率比使用repartition加sortBykey高，排序是在shuffle过程中进行，一边shuffle，一边排序
  */
object RepartitionAndSortWithinPartition_RDD {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("RepartitionAndSortWithinPartition_RDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    val list = List((1, 1), (2, 2), (1, 3), (5, 4), (3, 5), (7, 6))
    // 3.parallelize将集合转成rdd,repartitionAndSortWithinPartitions先重新分区，再对其进行排序
    val rdd1 = sc.parallelize(list).repartitionAndSortWithinPartitions(new HashPartitioner(3))
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
