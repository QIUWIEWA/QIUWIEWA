package com.stark.spark.Rdd

import org.apache.spark.{SparkConf, SparkContext}

object study01_map {
  def main(args: Array[String]): Unit = {
    //spark的环境配置
    val sparkConf = new SparkConf()
      .setAppName("scalaSpark")
      .setMaster("local[2]")

    //创建spark运行环境
    val sc = new SparkContext(sparkConf)

    //编写代码
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5))
    rdd.map(integer => integer + 1)
      .foreach(println)


    sc.stop()
  }
}
