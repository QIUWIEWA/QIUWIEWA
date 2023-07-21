package com.stark.spark.ActionRdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class use_reduce {
    public static void main(String[] args) {
        // 创建 SparkConf
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("sparkcore");

        // 创建 JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // 创建列表，作为初始 RDD 的数据
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> javaRDD = jsc.parallelize(list);

        // 使用 reduce 方法对 RDD 中的元素进行归约操作
        int sum = javaRDD.reduce((a, b) -> a + b);

        // 打印归约结果
        System.out.println("RDD 中元素的总和：" + sum);

        // 关闭 JavaSparkContext
        jsc.close();
    }
}