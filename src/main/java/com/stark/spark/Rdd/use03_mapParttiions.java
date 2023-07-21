package com.stark.spark.Rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * ---mapParttiions---
 * 简介：
 * 与map类似，将传进入的数据应用func再传出
 * 不同的是，mapParttiions是一次传入一个分区的数据，性能更好，
 * 但需要注意的是一旦一个分区的数据过庞大，会有oom的危险。慎用
 * map：一次一条，慢，安全
 * mapParttiions：一次一个分区，快，有风险
 * ---
 * 实例：
 *  javaRDD.mapPartitions(words -> {
 *     ArrayList<Tuple2<String,Integer>> res = new ArrayList<>();
 *
 *     while (words.hasNext()){
 *         String word = words.next();
 *         res.add(new Tuple2<>(word,1));
 *      }
 *          return res.iterator();
 *     }).collect().forEach(System.out::println);
 * 注：其传入的是一个迭代器，传出也要是迭代器
 * */

public class use03_mapParttiions {
    public static void main(String[] args) {
        //创建conf配置参数
        SparkConf sparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[2]");

        //创建spark运行环境
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //编写代码
        //编写代码
        List<String> list = Arrays.asList("a", "b", "c", "d", "e");
        JavaRDD<String> javaRDD = jsc.parallelize(list,2);

        javaRDD.mapPartitions(words -> {
            ArrayList<Tuple2<String,Integer>> res = new ArrayList<>();

            while (words.hasNext()){
                String word = words.next();
                res.add(new Tuple2<>(word,1));
            }

            return res.iterator();
        }).collect().forEach(System.out::println);

        //关闭资源
        jsc.stop();
    }
}
