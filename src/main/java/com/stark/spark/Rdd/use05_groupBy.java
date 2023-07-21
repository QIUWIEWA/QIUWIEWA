package com.stark.spark.Rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

/**
 * ---groupBY---
 * 简介：
 *从 RDD 中的每个元素中提取一个键，按照这个键对元素进行分组，然后返回
 * ---
 * 实例：
 *                                                                       (a,[apple,and])
 * List("apple", "banana", "cherry","and")  --groupBy(chatAt(0))-->      (b,[banana])
 *                                                                       (c,[cherry])
 * ---
 * 代码：
 * javaRDD.groupBy(word  -> word.chatAt(0));
 * */
public class use05_groupBy {
    public static void main(String[] args) {
        //创建conf配置参数
        SparkConf sparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[2]");

        //创建spark运行环境
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //编写代码
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> javaRDD = jsc.parallelize(list);

        //利用分组，分开奇数和偶数
//        javaRDD.groupBy(new Function<Integer, Integer>() {
//            @Override
//            public Integer call(Integer integer) throws Exception {
//                return integer % 2 ;
//            }
//        }).collect().forEach(System.out::println);


        //lambda优化版，（group by需要一次落盘 来进行shuffle，可以指定分区，需要一次落盘）
        javaRDD.groupBy(integer -> integer % 2);

        //关闭资源
        jsc.stop();
    }
}
