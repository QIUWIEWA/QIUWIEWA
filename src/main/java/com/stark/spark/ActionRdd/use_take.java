package com.stark.spark.ActionRdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
/**
 *
 Spark 提供了许多常见的行动算子（Action Operators）用于触发对 RDD 或 DataFrame 的计算并返回结果。以下是一些常见的 Spark 行动算子：


 aggregate(zeroValue)(seqOp, combOp)：使用两个不同的函数对 RDD 中的元素进行聚合操作。
 max()：返回 RDD 或 DataFrame 中的最大值。
 min()：返回 RDD 或 DataFrame 中的最小值。
 sum()：计算 RDD 或 DataFrame 中所有数值元素的总和。
 mean()：计算 RDD 或 DataFrame 中所有数值元素的平均值。

 这些行动算子是
 * */
public class use_take {
    public static void main(String[] args) {
        // 创建 SparkConf
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("sparkcore");

        // 创建 JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // 创建列表，作为初始 RDD 的数据
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> javaRDD = jsc.parallelize(list);

        // 使用 take 方法获取 RDD 中指定数量的元素
        int count = 3;
        List<Integer> takenElements = javaRDD.take(count);

        // 打印取出的元素
        System.out.println("RDD 中取出的元素：");
        for (Integer element : takenElements) {
            System.out.println(element);
        }

        // 关闭 JavaSparkContext
        jsc.close();
    }
}
