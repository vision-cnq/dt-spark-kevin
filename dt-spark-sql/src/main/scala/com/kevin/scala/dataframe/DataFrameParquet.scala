package com.kevin.scala.dataframe

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 读取json文件并保存成parquet文件和加载parquet文件
  */
object DataFrameParquet {

  def main(args: Array[String]): Unit = {
    // 1.创建sparkconf
    val conf = new SparkConf().setAppName("DataFrameParquet").setMaster("local")
    // 2.创建sc
    val sc = new SparkContext(conf)
    // 3.创建sqlContext
    val sqlContext = new SQLContext(sc)
    val json = sc.textFile("DTSparkSql\\src\\main\\resources\\json")
    // 4.读取json文件将文件转成rdd
    val df = sqlContext.read.json(json)

    // SaveMode指定存储文件时的保存模式：Overwrite：覆盖，Append：追加，ErrorIfExists：如果存在就报错，Ignore：若果存在就忽略
    // 5.将DataFrame保存成parquet文件，保存成parquet文件只能使用Overwrite和Ignore两种方式
    df.write.mode(SaveMode.Overwrite).format("parquet").save("./sparksql/parquet")

    // 6.加载parquet文件成df，加载parquet文件只能使用下面两种
    val load = sqlContext.read.parquet("./sparksql/parquet")
    // val load = sqlContext.read.format("parquet").load("./sparksql/parquet")

    // 7.显示表中所有的数据
    load.show()
    // 8.关闭sc
    sc.stop()

  }

}
