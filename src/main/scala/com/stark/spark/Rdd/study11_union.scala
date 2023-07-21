package com.stark.spark.Rdd

import org.apache.spark.{SparkConf, SparkContext}

object study11_union {
  def main(args: Array[String]): Unit = {
    //spark的环境配置
    val sparkConf = new SparkConf()
      .setAppName("scalaSpark")
      .setMaster("local[2]")

    //创建spark运行环境
    val sc = new SparkContext(sparkConf)

    //编写代码
    val rdd1 = sc.parallelize(List("apple", "banana", "cherry"))

    // 创建第二个 JavaRDD// 创建第二个 JavaRDD
    val rdd2 = sc.parallelize(List("orange", "kiwi"))

    rdd1.union(rdd2).foreach(println)

    sc.stop()

  }
}
