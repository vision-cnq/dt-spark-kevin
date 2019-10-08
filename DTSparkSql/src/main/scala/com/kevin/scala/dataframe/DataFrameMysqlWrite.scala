package com.kevin.scala.dataframe

import java.util.Properties

import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *  将数据写入mysql
  */
object DataFrameMysqlWrite {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("DataFrameMysqlWrite").setMaster("local")
    // 2.创建sc
    val sc = new SparkContext(conf)
    // 3.创建sqlContext
    val sqlContext = new SQLContext(sc)
    // 4.通过并行化创建RDD
    val person = sc.parallelize(Array("3 cao 23","4 zheng 20","5 mai 20")).map(_.split(" "))
    // 5.通过StructType直接指定每个字段的schema
    val schema = StructType(List(
        StructField("id",IntegerType,true),
        StructField("name",StringType,true),
        StructField("age",IntegerType,true)
    ))
    // 6.将rdd映射到rowRdd
    val row = person.map(p => Row(p(0).toInt, p(1).trim, p(2).toInt))
    // 7.创建DataFrame
    val df = sqlContext.createDataFrame(row,schema)
    // 8.数据库的账号和密码
    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","Hadoop01!")
    // 9.将数据插入表中，SaveMode.Overwrite覆盖表的数据
    df.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://192.168.171.101:3306/test","person",prop)
    // 10.关闭sc
    sc.stop()

  }

}
