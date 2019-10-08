package com.kevin.scala.dataframe

import org.apache.spark.sql.types._
import org.apache.spark.sql.{RowFactory, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 动态创建Scheme将非json格式RDD转换成DataFrame，推荐
  */
object DataFrameRDDWithStruct {

  def main(args: Array[String]): Unit = {
    // 1.创建sparkconf
    val conf = new SparkConf().setAppName("DataFrameRDDWithStruct").setMaster("local")
    // 2.创建sc
    val sc = new SparkContext(conf)
    // 3.创建sqlcontext
    val sqlContext = new SQLContext(sc)
    val line = sc.textFile("DTSparkSql\\src\\main\\resources\\person.txt")
    // 4.读取文件数据，先使用map切分数据，再使用map将数据转成Row类型
    val row = line.map(_.split(",")).map(s => RowFactory.create(s(0),s(1),Integer.valueOf(s(2))))
    // 5.动态构建DataFrame中的元数据，一般来说这里的字段可以源于字符串，也可以源于外部数据库
    val structFields = Array(
      StructField("id",StringType,true),
      StructField("name",StringType,true),
      StructField("age",IntegerType,true)
    )
    // 6.将list转为DataFrame中的元数据
    val structType = DataTypes.createStructType(structFields)
    // 7.将row类型的rdd数据和对应的字段名称类型转成DataFrame
    val df = sqlContext.createDataFrame(row,structType)
    // 8.查询数据
    df.show()
    // 9.将df转成rdd然后遍历获取name的数据
    df.rdd.foreach(r => println(r.getAs("name")))
    // 10.关闭sc
    sc.stop()

  }

}
