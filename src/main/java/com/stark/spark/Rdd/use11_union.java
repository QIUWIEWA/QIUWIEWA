package com.stark.spark.Rdd;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * ---union---
 * 连接两表
 * 请注意，union 操作只是将两个 RDD 中的元素简单地合并起来，而不会进行任何列结构的调整或转换。
 * 因此，确保两个 RDD 的列结构相匹配非常重要，否则可能会导致结果不符合预期。
 * */

public class use11_union {
    public static void main(String[] args) {
        // 创建 SparkConf 配置对象
        SparkConf sparkConf = new SparkConf()
                .setAppName("UnionExample")
                .setMaster("local[*]");

        // 创建 JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // 创建第一个 JavaRDD
        JavaRDD<String> rdd1 = jsc.parallelize(Arrays.asList("apple", "banana", "cherry"));

        // 创建第二个 JavaRDD
        JavaRDD<String> rdd2 = jsc.parallelize(Arrays.asList("orange", "kiwi"));

        // 合并两个 RDD
        JavaRDD<String> unionRDD = rdd1.union(rdd2);

        // 打印合并结果
        unionRDD.foreach(System.out::println);

        // 关闭 JavaSparkContext
        jsc.close();
    }
}