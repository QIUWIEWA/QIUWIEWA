package com.stark.spark.Rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.elasticsearch.common.collect.Tuple;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * ---tuple---
 * 简介:
 * 元组（Tuple）是一种不可变的数据结构,可以包含从 2 到 22 个元素。
 * _1、_2、_3 分别访问第一个、第二个和第三个元素,以此类推
 *----
 *  实例：
 *  javaRDD.map(word -> new Tuple<String,Integer>(word,1));
 *  即 a -> (a,1)
 * */

public class use02_map_tuple {
    public static void main(String[] args) {
        //创建conf配置参数
        SparkConf sparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[2]");

        //创建spark运行环境
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //编写代码
        List<String> list = Arrays.asList("A", "B", "C", "D", "F");

        JavaRDD<String> javaRDD = jsc.parallelize(list);
        javaRDD.map(s -> new Tuple2<String, Integer>(s,1));
//        javaRDD.map(new Function<String, Object>() {
//            @Override
//            public Object call(String s) throws Exception {
//                return new Tuple2<>(s,1);
//            }
//        });


        //关闭资源
        jsc.stop();
    }
}
