package com.stark.spark.Rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
/**
 * ---flatMap---
 * 简介：
 * 将一行拆分后变成多行
 * 传入一个元素，传出一个迭代器
 *                                               "HELLO"
 * "HELLO WORLD WOC".split(" ")  --flatmap-->    "WORLD"
 *                                               "WOC"
 * ---
 * 实例：
 * javaRDD.flatMap(line ->{
 *     ArrayList<Tuple2<String,Integer>> result = new ArrayList<>();
 *     String[] split = line.split(" ");
 *     for(String word:split){
 *         result.add(new Tuple2<>(word,1));
 *     }
 *     return result.iterator();
 * }).collect().forEach(System.out::println);
 *  */

public class use04_flatMap {
    public static void main(String[] args) {
        //创建conf配置参数
        SparkConf sparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[2]");

        //创建spark运行环境
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //编写代码
        List<String> list = Arrays.asList("holle world", "holle spark");
        JavaRDD<String> javaRDD = jsc.parallelize(list);
        //创建rdd(javaRDD或JavaPairRDD)

//        传入一行的数据。用来切割
        javaRDD.flatMap(line ->{
            ArrayList<Tuple2<String,Integer>> result = new ArrayList<>();
            String[] split = line.split(" ");
            for(String word:split){
                result.add(new Tuple2<>(word,1));
            }
            return result.iterator();
        }).collect().forEach(System.out::println);

        //关闭资源
        jsc.stop();
    }
}
