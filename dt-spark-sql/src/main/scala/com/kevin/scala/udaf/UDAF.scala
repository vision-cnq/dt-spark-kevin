package com.kevin.scala.udaf

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}
import org.apache.spark.sql.{Row, RowFactory, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * UDAF用户自定义聚合函数   多对一
  *   例子：需求：实现统计相同name值的个数
  */
object UDAF {

  def main(args: Array[String]): Unit = {
    // 1.创建sparkconf
    val conf = new SparkConf().setAppName("UDAF").setMaster("local")
    // 2.创建sc
    val sc = new SparkContext(conf)
    // 3.创建sqlcontext
    val sqlContext = new SQLContext(sc)
    // 4.创建list将list转成rdd
    val line = sc.parallelize(List("zhangsan", "lisi", "wangwu", "maliu", "wangwu"))
    // 5.将RDD数据转成row，每一行row都只有一个数据
    val row = line.map(RowFactory.create(_))
    // 6.动态创建Schema方式加载DataFrame，叫做name
    val fields = Array(DataTypes.createStructField("name",DataTypes.StringType,true))
    // 7.将fields转为DataFrame中的元数据
    val schema = DataTypes.createStructType(fields)
    // 8.将row类型的rdd数据和对应的字段名称类型转成DataFrame，并注册成一张临时表
    sqlContext.createDataFrame(row,schema).registerTempTable("user")
    // 9.注册一个udaf函数
    sqlContext.udf.register("StringCount",new MyUDAF)
    // 10.使用UDAF函数统计相同值的个数
    sqlContext.sql("select name,StringCount(name) as strCount from user group by name").show()
    // 11.关闭sc
    sc.stop()

  }

}

/**
  * 自定义一个类集成UserDefinedaggregateFunction类
  */
class MyUDAF extends UserDefinedAggregateFunction {

  // 指定输入字段的字段及类型
  override def inputSchema: StructType = {
    DataTypes.createStructType(Array(
      DataTypes.createStructField("namexxx",DataTypes.StringType,true)))
  }

  // 在进行聚合操作的时候所要处理的数据的结果的类型
  override def bufferSchema: StructType = {
    DataTypes.createStructType(Array(DataTypes.createStructField("bffer",DataTypes.IntegerType,true)))
  }

  // 指定UDAF函数计算后返回的结果类型
  override def dataType: DataType = {
    DataTypes.IntegerType
  }

  // 确保一致性，一般用true，用以标记针对给定的一组输入，UDAF是否总是生成相同的结果
  override def deterministic: Boolean = {
    true
  }

  // 初始化一个内部自定义的值，在Aggregate之前每组数据的初始化结果
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0,0)
  }

  // 更新,每有一条数据参与运算就更新一下中间结果(update相当于在每一个分区中的计算)
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0,buffer.getInt(0)+1)
  }

  // 合并,全局级别的Merge操作
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0,buffer1.getInt(0)+buffer2.getInt(0))
  }

  // 最后返回一个和dataType方法的类型要一致的类型，返回UDAFzu最后的计算结果
  override def evaluate(row: Row): Any = {
    row.getInt(0)
  }

}

