package com.kevin.scala

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 线性回归
  */
object LinearRegression {

  def main(args: Array[String]): Unit = {

    // 构建spark对象
    val conf = new SparkConf().setAppName("LinearRegression").setMaster("local")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    // 读取样本数据
//    val data_path = "DTSparkMLlib\\src\\main\\resources\\lpsa.data"
    val data_path = "lpsa.data"
    val data = sc.textFile(data_path)
    val examples = data.map(line => {
      val parts = line.split(",")
      val y = parts(0)  // y，有y则是监督训练数据，没有y是无监督
      val xs = parts(1) // 维度
      LabeledPoint(parts(0).toDouble,Vectors.dense(parts(1).split(" ").map(_.toDouble)))
    })

    // 将rdd数据分成0.8(训练集)一份，0.2(测试集)一份，种子1是固定分配
    val train2TestData = examples.randomSplit(Array(0.8,0.2),1)

    /**
      * 迭代次数
      * 训练一个多元线性回归模型收敛（停止迭代）条件：
      *     1.error值小于用户指定的error值
      *     2.达到一定的迭代次数
      */
    val numIterations = 100
    // 在每次迭代的过程中，梯度下降算法的下降步长大小 0.1,0.2,0.3,0.4
    val stepSize = 1

    val miniBatchFraction = 1
    // 创建线性回归模型
    val lrs = new LinearRegressionWithSGD()
    // 截距,让训练出来的模型有w0参数，就是有截距   true有截距,false无截距
    lrs.setIntercept(true)
    // 设置步长
    lrs.optimizer.setStepSize(stepSize)
    // 设置迭代次数
    lrs.optimizer.setNumIterations(numIterations)
    // 每一次下山后，是否计算所有样本的误差值,1代表所有样本,默认就是1.0
    lrs.optimizer.setMiniBatchFraction(miniBatchFraction)

    val model = lrs.run(train2TestData(0))
    println(model.weights)  // 权重，维度
    println(model.intercept)  // 截距

    // 对样本进行测试
    // 预测测试集,features是维度,取维度预测y值
    val prediction = model.predict(train2TestData(1).map(_.features))
    // 将预测出来测试集中的y值和测试集的y值压缩在一起
    val prediction_label = prediction.zip(train2TestData(1).map(_.label))

    // 取前二十条
    val print_predict = prediction_label.take(20)
    println("prediction"+"\t"+"lable")

    for (i <- 0 to print_predict.length-1) {
      // 打印预测的值，真实的值
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }

    // 计算测试集平均误差
    val loss = prediction_label.map{
      case (p,v) =>
        val err = p - v
        Math.abs(err) // 取绝对值的差和
    }.reduce(_+_)

    // 绝对值的差和/所有的点得到误差值
    val error = loss / train2TestData(1).count
    println("Test RMSE = " + error)
    // 模型保存
    //    val model_path = "model"
    //    model.save(sc,model_path)
    //    val same_model = LinearRegressionModel.load(model_path)
    sc.stop()


  }

}
