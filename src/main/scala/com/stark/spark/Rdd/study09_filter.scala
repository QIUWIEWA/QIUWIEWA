package com.stark.spark.Rdd

import org.apache.spark.{SparkConf, SparkContext}

object study09_filter {
  def main(args: Array[String]): Unit = {
    //spark的环境配置
    val sparkConf = new SparkConf()
      .setAppName("scalaSpark")
      .setMaster("local[2]")

    //创建spark运行环境
    val sc = new SparkContext(sparkConf)

    //编写代码
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9))

    rdd.filter(integer => integer%2 ==1)
      .foreach(println)



    sc.stop()

  }
}
