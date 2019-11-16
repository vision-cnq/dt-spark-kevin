package com.kevin.scala.tuning

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 尽量避免使用shuffle类算子
  */
object ShuffleRDD {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("ShuffleRDD").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.parallelize将集合转成rdd
    val rdd1 = sc.parallelize(List((0, "aa"),(1,"a"),(2,"b"),(3,"c"),(4,"d"),(5,"f")))
    val rdd2 = sc.parallelize(List((1, 100),(2, 200),(3, 300),(4, 400)))
    // 4.join,将K相同的V值连接在一起，返回两边都拥有的值
    // rdd1.join(rdd2).foreach(value => (println("join: "+ value)))

    // join会发生shuffle操作。优化：broadcast+map代替join，不会发生shuffle操作
    // 先使用broadcast将数据量较小的rdd作为广播变量(尽量不超过1G)
    val rdd2Broadcast = sc.broadcast(rdd2.collect())

    // 从rdd2Broadcast获取所有的rdd2数据，然后遍历判断，如果rdd1和rdd2的key相同，则拼接在一起(join)
    def function(tuple: (Int,String)): (Int,(String,String)) ={
      for(value <- rdd2Broadcast.value){  // 遍历
        if(value._1.equals(tuple._1)) // 判断rdd1和rdd2的key是否相同
          return (tuple._1,(tuple._2,value._2.toString))  // 相同则join
      }
      (tuple._1,(tuple._2,null))
    }

    val rdd3 = rdd1.map(function(_))
    rdd3.foreach(result => println(result._1+","+result._2))

    // 关闭sc
    sc.stop()

  }

}
