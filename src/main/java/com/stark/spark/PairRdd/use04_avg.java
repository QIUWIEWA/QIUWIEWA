package com.stark.spark.PairRdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * ---avg---
 * 简介：
 * 并没有一个单独的算子，而是利用mapValues和reduceByKey相加起来的一种算法
 * ----
 * 实例：
 * 原始数据("A",12) ("A",11)
 * 先将数据转换成  （"A",(12,1)）  ("A",(11,1))
 * 然后对两个聚合   ("A",(23,2))
 * 然后在相除23/2
 * 代码如下
 * */

public class use04_avg {
    public static void main(String[] args) {
        //创建conf配置参数
        SparkConf sparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[2]");

        //创建spark运行环境
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //编写代码

        ArrayList<Tuple2<String,Integer>> list= new ArrayList<>();
        list.add(new Tuple2<>("A",12));
        list.add(new Tuple2<>("B",14));
        list.add(new Tuple2<>("C",16));
        list.add(new Tuple2<>("D",14));
        list.add(new Tuple2<>("D",14));
        list.add(new Tuple2<>("B",11));
        list.add(new Tuple2<>("D",17));
        list.add(new Tuple2<>("D",15));
        list.add(new Tuple2<>("A",11));
        JavaPairRDD<String, Integer> pairRDD = (JavaPairRDD<String, Integer>) jsc.parallelizePairs(list);


//        先将数据转换成  （"A",(12,1)）
//        然后对两个聚合   ("A",(23,2))
//        然后在相除
        pairRDD.mapValues(integer -> new Tuple2<>(integer,1))
                .reduceByKey((sum,elem) -> new Tuple2<>(sum._1+elem._1,sum._2+elem._2))
                .mapValues(v1 -> v1._1.doubleValue()/v1._2)
                .collect()
                .forEach(System.out::println);

        //关闭资源
        jsc.stop();
    }
}
