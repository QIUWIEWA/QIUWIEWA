package com.stark.spark.Rdd

import org.apache.spark.{SparkConf, SparkContext}

object study10_join {
  def main(args: Array[String]): Unit = {
    //spark的环境配置
    val sparkConf = new SparkConf()
      .setAppName("scalaSpark")
      .setMaster("local[2]")

    //创建spark运行环境
    val sc = new SparkContext(sparkConf)

    //编写代码
    val rdd1 = sc.parallelize(List(
      (1, "a"),
      (2, "b"),
      (3, "c"),
      (4, "d"),
      (5, "e")
    ))

    val rdd2 = sc.parallelize(List(
      (3, 33),
      (1, 22),
      (2, 21),
      (4, 12),
      (5, 23)
    ))

    rdd1.join(rdd2).foreach(println)

    sc.stop()

  }
}
