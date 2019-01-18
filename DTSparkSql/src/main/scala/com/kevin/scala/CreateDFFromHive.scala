/*
package com.kevin.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object CreateDFFromHive {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("HiveSource").setMaster("spark://192.168.171.101:9083")
    val sc = new SparkContext(conf)
    /**
     * HiveContext是SQLContext的子类。
     */
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("show tables").collect().foreach(println)

    sc.stop()
  }
}*/
