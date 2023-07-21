package com.stark.spark.Rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * ---mapToPair---
 * 简介：
 * 将一个 RDD 中的元素转换为键值对（key-value pair）形式的元素
 * 将每个输入元素转换为一个键值对，并返回一个 JavaPairRDD
 * */

public class use12_mapToPair {
    public static void main(String[] args) {
        // 创建 SparkConf 配置对象
        SparkConf sparkConf = new SparkConf()
                .setAppName("MapToPairExample")
                .setMaster("local[*]");

        // 创建 JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // 创建 JavaRDD
        JavaRDD<String> rdd = jsc.parallelize(Arrays.asList("apple", "banana", "cherry"));

        // 将每个元素转换为键值对
        JavaPairRDD<String, Integer> pairRDD = rdd.mapToPair(word -> new Tuple2<>(word, word.length()));

        // 打印转换后的结果
        pairRDD.foreach(System.out::println);

        // 关闭 JavaSparkContext
        jsc.close();
    }
}
