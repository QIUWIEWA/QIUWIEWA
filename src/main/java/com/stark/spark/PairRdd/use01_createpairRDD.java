package com.stark.spark.PairRdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
/**
 * ---createPairRDD---
 * 创建一个键值对的rdd，如下
 * */
public class use01_createpairRDD {
    public static void main(String[] args) {
        //创建conf配置参数
        SparkConf sparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[2]");

        //创建spark运行环境
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //编写代码
        ArrayList<Tuple2<String,Integer>> list= new ArrayList<>();
        list.add(new Tuple2<>("A",1));
        list.add(new Tuple2<>("B",1));
        list.add(new Tuple2<>("C",1));
        list.add(new Tuple2<>("D",1));
        list.add(new Tuple2<>("A",1));
        JavaPairRDD<String, Integer> pairRDD = (JavaPairRDD<String, Integer>) jsc.parallelizePairs(list);

        //创建rdd(javaRDD或JavaPairRDD)
        pairRDD.collect().forEach(System.out::println);


        //关闭资源
        jsc.stop();
    }
}
