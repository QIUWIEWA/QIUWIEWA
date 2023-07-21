package com.stark.spark.Rdd;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
/**
 * ---filter---
 * 简介：
 * 根据返回值，对元素进行过滤
 * 输入一行元素，输出一个Boolean的类型的值
 * 实例：
 * [(a,1),(b,2),(c,1),(d,2)] --filter(tu._2 == 1)-->  [(a,1),(c,1)]
 * 代码：
 * JavaRDD<Integer> filteredRDD = rdd.filter(tuple -> tuple._2 == 1);
 * */
public class use09_filter {
    public static void main(String[] args) {
        // 创建 SparkConf 配置对象
        SparkConf sparkConf = new SparkConf()
                .setAppName("FilterExample")
                .setMaster("local[*]");

        // 创建 JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // 创建 JavaRDD
        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        // 使用 filter 进行筛选
        JavaRDD<Integer> filteredRDD = rdd.filter(num -> num % 2 == 0);

        // 打印筛选结果
        filteredRDD.foreach(System.out::println);

        // 关闭 JavaSparkContext
        jsc.close();
    }
}
