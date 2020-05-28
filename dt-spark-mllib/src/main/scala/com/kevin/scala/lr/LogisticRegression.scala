package com.kevin.scala.lr

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 逻辑回归 健康状况训练集
  */
object LogisticRegression {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LogisticRegression").setMaster("local")
    val sc = new SparkContext(conf)
    // 读取样本数据
    val data_path = "DTSparkMLlib\\src\\main\\resources\\健康状况训练集.txt"
    // 加载 LIBSVM 格式的数据  这种格式特征前缀要从1开始
    val inputData = MLUtils.loadLibSVMFile(sc,data_path)
    // 将数据分成训练集(0.7)和测试集(0.3)
    val splits = inputData.randomSplit(Array(0.7,0.3),seed = 1L)
    // 创建逻辑回归模型
    val lr = new LogisticRegressionWithLBFGS()
    // 执行训练集
    val model = lr.run(splits(0))
    val result = splits(1).map{point => Math.abs(point.label-model.predict(point.features))}

    println("正确率: "+(1.0-result.mean()))

    // 逻辑回归算法训练出来的模型，模型中的参数个数（w0....w6）=训练集中特征数(6)+1
    println(model.weights.toArray.mkString(" "))
    println(model.intercept)

    sc.stop()

  }

}
