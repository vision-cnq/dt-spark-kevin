package com.kevin.scala.tuning

import org.apache.spark.{SparkConf, SparkContext}


/**
  * 使用Kryo优化序列化性能
  */
object KryoTuning {

  def main(args: Array[String]): Unit = {

    // 创建SparkConf
    val conf = new SparkConf().setAppName("KryoTuning").setMaster("local")
    // 设置Spark序列化方式为Kryo
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    // 注册要序列化的自定义类型
    conf.registerKryoClasses(Array(classOf[ClassDemo1], classOf[ClassDemo2]))
    // 创建SparkContext
    val sc = new SparkContext(conf)
    // 如果注册的要序列化自定义的类型本身非常大，比如属性有上百个，那么就会导致序列化的对象过大。
    // 此时需要对Kryo本身进行优化，因为Kryo内部的缓存可能不够存放那么大的class对象，此时就需要调用SparkConf.set()
    // 设置spark.kryoSerializer.buffer.mb参数的值，将其调大。默认为2，就是最大能缓存2M的对象，然后序列化，我们可以加大缓存上限
    sc.stop()

  }

}

class ClassDemo1{
  val field1 = ""
  val field2 = ""
  val field3 = ""
  val field4 = ""
  val field5 = ""
  // ...
}

class ClassDemo2{

}
