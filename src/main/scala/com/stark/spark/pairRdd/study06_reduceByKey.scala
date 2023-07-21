package com.stark.spark.pairRdd

import org.apache.spark.{SparkConf, SparkContext}

object study06_reduceByKey {
  def main(args: Array[String]): Unit = {
    //spark的环境配置
    val sparkConf = new SparkConf()
      .setAppName("scalaSpark")
      .setMaster("local[2]")

    //创建spark运行环境
    val sc = new SparkContext(sparkConf)

    //编写代码
    val rdd = sc.parallelize(List(
      ("s", 1),
      ("a", 2),
      ("c", 3),
      ("c", 3),
      ("c", 3),
      ("c", 3),
      ("r", 6)
    )).mapValues(f => (f, 1))
      .reduceByKey((sum, elem) => (sum._1 + elem._1, sum._2 + elem._2))
    sc.stop()

  }
}
