package com.stark.spark.PairRdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * ---mapValues---
 * 简介：
 * 只对键值对的值进行操作，对键不做修改
 * ---
 * 代码：
 * pairRDD.mapValues(integer -> integer * 2);
 * */

public class use02_mapValues {
    public static void main(String[] args) {
        //创建conf配置参数
        SparkConf sparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[2]");
        ArrayList<Tuple2<String,Integer>> list= new ArrayList<>();
        //创建spark运行环境
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //编写代码
        list.add(new Tuple2<>("A",1));
        list.add(new Tuple2<>("B",1));
        list.add(new Tuple2<>("D",1));
        list.add(new Tuple2<>("D",1));
        list.add(new Tuple2<>("A",1));
        JavaPairRDD<String, Integer> pairRDD = (JavaPairRDD<String, Integer>) jsc.parallelizePairs(list);

        //mapValues :只对pairRDD的value进行操作，保留key不动
        pairRDD.mapValues(integer -> integer * 2).collect().forEach(System.out::println);

        //创建rdd(javaRDD或JavaPairRDD)


        //关闭资源
        jsc.stop();
    }
}
