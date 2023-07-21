package com.stark.spark.ActionRdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * ---aggregate---
 * 简介：
 * 用于对 RDD 中的元素进行聚合操作，并返回一个单一的结果。
 * ---
 * 参数：
 * 初始值（zeroValue）：这是一个初始的累加值，对于每个分区内的聚合操作和不同分区之间的聚合操作都会使用到它。
 * 分区内聚合函数（seqOp）：这是一个函数，用于在每个分区内对元素进行聚合操作。它接收两个参数，第一个参数是累加值（初始值或前一个聚合结果），第二个参数是当前分区内的元素。函数的返回值是一个新的累加值。
 * 分区间聚合函数（combOp）：这是一个函数，用于对不同分区之间的聚合结果进行聚合操作。它接收两个参数，分别是两个聚合结果，返回一个新的聚合结果。
 * ---
 * 执行过程：
 * 1.对每个分区内的元素进行分别的聚合操作，使用初始值和分区内聚合函数。这会得到每个分区的聚合结果。
 * 2.对不同分区的聚合结果进行分区间聚合操作，使用分区间聚合函数。这会得到最终的聚合结果。
 * 3.最终的聚合结果将作为算子的返回值。
 * */
public class use_aggregate {
    public static void main(String[] args) {
        // 创建 SparkConf
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("sparkcore");

        // 创建 JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // 创建列表，作为初始 RDD 的数据
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> javaRDD = jsc.parallelize(list, 2); // 设置分区数为 2

        // 使用 aggregate 方法进行聚合操作
        int zeroValue = 0;
        int sum = javaRDD.aggregate(
                zeroValue,
                (acc, num) -> acc + num,
                (acc1, acc2) -> acc1 + acc2
        );

        // 打印聚合结果
        System.out.println("RDD 中元素的总和：" + sum);

        // 关闭 JavaSparkContext
        jsc.close();
    }
}