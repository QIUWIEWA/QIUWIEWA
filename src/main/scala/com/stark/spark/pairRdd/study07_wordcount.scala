package com.stark.spark.pairRdd

import org.apache.spark.{SparkConf, SparkContext}

object study07_wordcount {
  def main(args: Array[String]): Unit = {
    //spark的环境配置
    val sparkConf = new SparkConf()
      .setAppName("scalaSpark")
      .setMaster("local[2]")

    //创建spark运行环境
    val sc = new SparkContext(sparkConf)

    //编写代码
    val rdd = sc.parallelize(List(
      "hello world woc haha",
      "my name is spark",
      "can you help me",
      "haha i can not",
      "haha woc "
    )).flatMap(word => word.split(" "))
      .map(word => (word,1))
      .reduceByKey((sum,elem) => sum+elem)
      .foreach(println)

    sc.stop()

  }
}
