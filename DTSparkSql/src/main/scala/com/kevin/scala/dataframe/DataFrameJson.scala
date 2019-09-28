package com.kevin.scala.dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 读取json格式文件转为DataFrame表形式分析处理
  *  json文件中不能嵌套json格式的内容
  *  1.读取json格式两种方式
  *  2.df.show()默认显示前20行，使用df.show(行数)显示多行
  *  3.df.javaRDD/(scala df.rdd)将DataFrame转换成RDD
  *  4.df.printSchema()显示DataFrame中Schema信息
  *  5.dataFrame自带的API操作DataFrame，一般不使用
  *  6.使用sql查询，先将DataFrame注册成临时表：df.registerTempTable("jtable")，
  *  再使用sql,怎么使用sql?sqlContext.sql("sql语句")
  *  7.不能加载嵌套的json文件
  *  8.df加载过来之后将列安装ascii排序
  */
object DataFrameJson {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("DataFrameJson").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.创建SQLContext对象对sql进行分析处理
    val sqlContext = new SQLContext(sc)
    val file = "DTSparkSql\\src\\main\\resources\\json"
    // 4.读取json文件并转成DataFrame表
    val df = sqlContext.read.json(file)
    // 5.获取表中所有的数据
    df.show()
    // 6.使用DataFrame自带的API操作DataFrame
    // select name from table
    df.select("name").show()

    // select name, age+10 as addage from table
    df.select(df.col("name"),df.col("age").plus(10).alias("addage")).show()

    // select name ,age from table where age>19
    df.select(df.col("name"),df.col("age")).where(df.col("age").gt("19")).show()

    // select age,count(*) from table group by age
    df.groupBy(df.col("age")).count().show()

    // 7.将DataFrame注册成临时表使用sql语句操作
    df.registerTempTable("tempTable")
    sqlContext.sql("select age,count(*) as gg from tempTable group by age").show()
    sqlContext.sql("select name,age from tempTable").show()

    // 8.关闭
    sc.stop()
  }

}
