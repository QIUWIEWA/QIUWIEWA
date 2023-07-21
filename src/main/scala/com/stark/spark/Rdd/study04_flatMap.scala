package com.stark.spark.Rdd

import org.apache.spark.{SparkConf, SparkContext}

object study04_flatMap {
  def main(args: Array[String]): Unit = {
    //spark的环境配置
    val sparkConf = new SparkConf()
      .setAppName("scalaSpark")
      .setMaster("local[2]")

    //创建spark运行环境
    val sc = new SparkContext(sparkConf)

    //编写代码
    val rdd = sc.parallelize(List("hello world woc", "hello spark scala"))

    rdd.flatMap(word => word.split(" "))
      .foreach(println)

    sc.stop()


  }

}
