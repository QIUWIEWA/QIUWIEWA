package com.stark.spark.Rdd

import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

object study08_sortBy {
  def main(args: Array[String]): Unit = {
    //spark的环境配置
    val sparkConf = new SparkConf()
      .setAppName("scalaSpark")
      .setMaster("local[2]")

    //创建spark运行环境
    val sc = new SparkContext(sparkConf)

    //编写代码
    val rdd = sc.parallelize(List(1,2,3,4,5,6,7,1,2,3,4,6,1,2,5))


    val sortedRDD = rdd.sortBy(integer => integer,true,2)
      .collect()
      .foreach(println)


    sc.stop()

  }

}
