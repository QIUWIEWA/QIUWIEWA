package com.stark.spark.pairRdd

import org.apache.spark.{SparkConf, SparkContext}
/**
 * scala没有区分pair算子
 * 直接用算子装元组或者列表即可
 * */

object study01_createPairRDD {
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
      ("r", 6)
    ))
    rdd.foreach(println)

    sc.stop()


  }

}
