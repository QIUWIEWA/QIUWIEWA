package com.stark.spark.Rdd

import org.apache.spark.{SparkConf, SparkContext}

object study02_map_tuple {
  def main(args: Array[String]): Unit = {
    //spark的环境配置
    val sparkConf = new SparkConf()
      .setAppName("scalaSpark")
      .setMaster("local[2]")

    //创建spark运行环境
    val sc = new SparkContext(sparkConf)

    //编写代码
    val rdd = sc.parallelize(List("a", "b", "c", "d", "e"))
    rdd.map(word => new Tuple2(word,1))
      .foreach(println)


    sc.stop()


  }

}
