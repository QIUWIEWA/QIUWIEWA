package com.stark.spark.ActionRdd;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
/**
 * first
 * 行动算子
 * 返回 RDD 或 DataFrame 中的第一个元素。
 * */
public class use_first {
    public static void main(String[] args) {
        // 创建 SparkConf
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("sparkcore");

        // 创建 JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // 创建列表，作为初始 RDD 的数据
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> javaRDD = jsc.parallelize(list);

        // 使用 first 方法获取 RDD 中的第一个元素
        Integer firstElement = javaRDD.first();

        // 打印第一个元素
        System.out.println("RDD 中的第一个元素：" + firstElement);

        // 关闭 JavaSparkContext
        jsc.close();
    }
}