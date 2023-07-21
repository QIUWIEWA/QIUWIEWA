package com.stark.spark.Rdd

import org.apache.spark.{SparkConf, SparkContext}
/**
 * scala没有mapToPair算子
 * 直接使用map算子即可
 * */
object study12_mapToPair {
  def main(args: Array[String]): Unit = {
    //spark的环境配置
    val sparkConf = new SparkConf()
      .setAppName("scalaSpark")
      .setMaster("local[2]")

    //创建spark运行环境
    val sc = new SparkContext(sparkConf)

    //编写代码
    val rdd = sc.parallelize(List("apple", "banana", "cherry"))

    rdd.map(word => (word ,word.length)).foreach(println)
    sc.stop()

  }

}
