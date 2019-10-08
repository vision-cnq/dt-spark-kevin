package com.kevin.scala

import com.kevin.scala.dataframe.Person
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * 读取文件数据转为表形式查询
  */
object DataFrameTest {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("DataFrameTest").setMaster("local")
    // 2.创建SparkContext
    val sc = new SparkContext(conf)
    // 3.创建SQLContext对象对sql进行分析处理
    val sqlContext = new SQLContext(sc)
    // 4.导入隐饰操作，否则RDD无法调用toDF方法
    import sqlContext.implicits._
    val file = "DTSparkSql\\src\\main\\resources\\person.txt"
    // 5.textFile读取文件数据,map用逗号切分数据,再map将数据类型转成Person。注：Person需要使用单列对象 case class，才能使用toDF转成DataFrame类型
    val df = sc.textFile(file).map(_.split(",")).map(p => Person(p(0), p(1), p(2).trim.toInt)).toDF()
    // 6.以数据框形式查看表中的数据，类似：select * from person
    df.show()
    // 7.查看Schema结构数据
    df.printSchema()
    // 8.注册成临时表
    df.registerTempTable("person")
    // 9.查询自定义的sql语句的数据
    val sql = sqlContext.sql("select id,name,age from person where id=2")
    // 10.将sql df转成rdd，并使用foreach遍历数据
    sql.rdd.foreach(row => {
      println("根据字段名称获取数据，推荐...")
      println("id = " + row.getAs("id"))
      println("name = " + row.getAs("name"))
      println("age = " + row.getAs("age"))
      /*println("根据下标获取数据，不推荐...")
      println("name = "+ row.getAs(0))
      println("age = "+ row.getAs(1))
      println("id = "+ row.getAs(2))
      println("根据下标和数据类型获取数据，不推荐...")
      println("name = "+ row.getString(0))
      println("age = "+ row.getInt(1))
      println("id = "+ row.getString(2))*/
    })
    // 11.关闭sc
    sc.stop()

  }

}
