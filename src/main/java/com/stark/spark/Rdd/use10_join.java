package com.stark.spark.Rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
/**
 * ---join---
 * 简介：
 * 将两表连接，默认使用tuple._1来作为连接值
 * 可选择分区个数
 * */

public class use10_join {

    public static void main(String[] args) {
        // 创建 SparkConf 配置对象
        SparkConf sparkConf = new SparkConf()
                .setAppName("JoinExample")
                .setMaster("local[*]");

        // 创建 JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        List<Tuple2<Integer, String>> arr = Arrays.asList(
                new Tuple2<>(1, "Alice"),
                new Tuple2<>(2, "Bob"),
                new Tuple2<>(3, "Charlie")
        );
        List<Tuple2<Integer, Integer>> arr2 = Arrays.asList(
                new Tuple2<>(1, 25),
                new Tuple2<>(2, 30),
                new Tuple2<>(4, 35)
        );
        // 创建第一个 JavaPairRDD
        JavaPairRDD<Integer, String> rdd1 = (JavaPairRDD<Integer, String>) jsc.parallelizePairs(arr);

        // 创建第二个 JavaPairRDD
        JavaPairRDD<Integer, Integer> rdd2 =  (JavaPairRDD<Integer, Integer>) jsc.parallelizePairs(arr2);

//        // 指定连接条件并进行连接操作
//        JavaPairRDD<Integer, Tuple2<String, Integer>> joinedRDD = rdd1.join(rdd2,2);
//
//        // 打印连接结果
//        joinedRDD.foreach(tuple -> {
//            int key = tuple._1();
//            String name = tuple._2()._1();
//            int age = tuple._2()._2();
//            System.out.println(key + ": (" + name + ", " + age + ")");
//        });
//
//        // 关闭 JavaSparkContext
//        jsc.close();
    }

}
