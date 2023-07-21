package com.stark.spark.ActionRdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
/**
 * ---count---
 * 统计rdd中元素的数量
 *---
 * 实例
 * 统计columnIndex的个数
 *  long count = rdd.map(line -> line.split(",")[columnIndex]).count();
 *  System.out.println("列中的元素数量：" + count);
 *-
 * ---
 * 实例
 *  搭配groupBy使用，对相同的元素进行统计
 *  long count = df.groupBy("columnName").count().count();
 *  System.out.println("列中的元素数量：" + count);
 * */
public class use_count {
    public static void main(String[] args) {
        // 创建 SparkConf
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("sparkcore");

        // 创建 JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // 创建列表，作为初始 RDD 的数据
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> javaRDD = jsc.parallelize(list);

        // 使用 count 方法计算 RDD 中元素的数量
        long count = javaRDD.count();

        // 打印 RDD 中元素的数量
        System.out.println("RDD 中元素的数量：" + count);

        // 关闭 JavaSparkContext
        jsc.close();
    }
}