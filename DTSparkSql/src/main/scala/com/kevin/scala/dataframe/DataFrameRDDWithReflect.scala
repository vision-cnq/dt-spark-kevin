package com.kevin.scala.dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 通过反射的方式将非json格式的RDD转换成DataFrame，不推荐使用
  */
object DataFrameRDDWithReflect {

  def main(args: Array[String]): Unit = {
    // 1.创建sparkconf
    val conf = new SparkConf().setAppName("DataFrameRDDWithReflect").setMaster("local")
    // 2.创建sc
    val sc = new SparkContext(conf)
    // 3.创建sqlcontext
    val sqlContext = new SQLContext(sc)
    // 4.导入隐饰操作，否则RDD无法调用toDF方法
    import sqlContext.implicits._
    val line = sc.textFile("DTSparkSql\\src\\main\\resources\\person.txt")
    // 5.读取文件用map切分，再用map将数据装成Person类型，toDF转成DataFrame
    val df = line.map(_.split(",")).map(p => new Person(p(0),p(1),p(2).trim.toInt)).toDF
    // 6.注册成临时表
    df.registerTempTable("person")
    // 7.查询id=2的数据
    sqlContext.sql("select * from person where id = 2").show()
    // 8.将df转成rdd，map将数据封装到person中
    val map = df.rdd.map(r => new Person(r.getAs("id"),r.getAs("name"),r.getAs("age")))
    // 9.遍历person的数据
    map.foreach(println(_))
    // 10.关闭sc
    sc.stop
  }

}
