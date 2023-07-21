package com.stark.spark.Rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * ---map---
 * 转换算子
 * ---
 *简介：
 * 对输入的每一行通过func()，在返回出去。
 * ---
 * 实例
 * javaRDD.map(word -> word+",1")
 * 即 a 变成 a1
 * */
public class use01_map {
    public static void main(String[] args) {
        //创建conf配置参数

        SparkConf sparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[2]");
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        //创建spark运行环境
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //编写代码
        List<String> list = Arrays.asList("a", "b", "c", "d");
        JavaRDD<String> javaRDD = jsc.parallelize(list, 2);

        javaRDD.map(word -> word+",1")
                .collect()
                .forEach(System.out::println);


        //创建rdd(javaRDD或JavaPairRDD)


        //关闭资源
        jsc.stop();
    }
}