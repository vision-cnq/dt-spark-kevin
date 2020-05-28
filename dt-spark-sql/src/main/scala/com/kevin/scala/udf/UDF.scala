package com.kevin.scala.udf

import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{RowFactory, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * UDF用户自定义函数    一对一
  *   例子：需求：使用自定义UDF获取name的长度
  */
object UDF {

  def main(args: Array[String]): Unit = {
    // 1.创建sparkconf
    val conf = new SparkConf().setAppName("UDF").setMaster("local")
    // 2.创建sc
    val sc = new SparkContext(conf)
    // 3.创建sqlcontext
    val sqlContext = new SQLContext(sc)
    // 4.创建list将list转成rdd
    val line = sc.parallelize(List("zhangsan", "lisi", "wangwu", "maliu"))
    // 5.将RDD数据转成row，每一行row都只有一个数据
    val row = line.map(RowFactory.create(_))
    // 6.动态创建Schema方式加载DataFrame，叫做name
    val fields = Array(DataTypes.createStructField("name",DataTypes.StringType,true))
    // 7.将fields转为DataFrame中的元数据
    val schema = DataTypes.createStructType(fields)
    // 8.将row类型的rdd数据和对应的字段名称类型转成DataFrame，并注册成一张临时表
    sqlContext.createDataFrame(row,schema).registerTempTable("user")

    // 9.根据UDF函数参数的个数来决定是实现哪一个UDF ，UDF1,UDF2....
    // 参数1：UDF函数的名称，参数2：传入的数据类型，参数3：传出的数据类型
    sqlContext.udf.register("StrLen",(s : String) =>(s.length))
    // 10.查询sql语句，使用自定义UDF获取name的长度
    sqlContext.sql("select name,StrLen(name) as length from user").show()
    // 11.关闭sc
    sc.stop()

  }

}
