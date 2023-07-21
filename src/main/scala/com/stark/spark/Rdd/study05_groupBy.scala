package com.stark.spark.Rdd

import org.apache.spark.{SparkConf, SparkContext}

object study05_groupBy {
  def main(args: Array[String]): Unit = {
    //spark的环境配置
    val sparkConf = new SparkConf()
      .setAppName("scalaSpark")
      .setMaster("local[2]")

    //创建spark运行环境
    val sc = new SparkContext(sparkConf)

    //编写代码
    val rdd = sc.parallelize(List(
      List("a",1),
      List("b",1),
      List("c",1),
      List("b",1),
      List("c",1),
      List("d",1),
      List("a",1),
      List("a",1)
    ))
    rdd.groupBy(f => f(0))
      .foreach(println)


    sc.stop()

  }
}
