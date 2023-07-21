package com.stark.spark.PairRdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
/**
 * ---groupByKey---
 * 简介：
 * pairrdd独有的算子，对key进行分组，可以指定分区数，返回的还是pairRDD
 * */

public class use03_groupByKey {
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

//        pairrdd独有的算子，对key进行分组，可以指定分区数，返回的还是pairRDD
        JavaPairRDD<String, Iterable<Integer>> pairrdd = pairRDD.groupByKey(2);

        //关闭资源
        jsc.stop();
    }
}
