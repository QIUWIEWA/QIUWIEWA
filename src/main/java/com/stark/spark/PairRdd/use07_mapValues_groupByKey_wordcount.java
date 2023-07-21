package com.stark.spark.PairRdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * 使用groupByKey + mapValues 做词频统计
 * 实例：
 * 原始数据：        ("A",1) ("A",1)
 * 使用groupByKey后  (A,[1,,1])
 * 使用mapValues后   (A,2)
 * */
public class use07_mapValues_groupByKey_wordcount {
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
        list.add(new Tuple2<>("D",1));
        list.add(new Tuple2<>("B",1));
        list.add(new Tuple2<>("D",1));
        list.add(new Tuple2<>("D",1));
        list.add(new Tuple2<>("A",1));
        JavaPairRDD<String, Integer> pairRDD = (JavaPairRDD<String, Integer>)jsc.parallelizePairs(list);

        //词频统计，1先按key进行分组 结果举例(A,[1,,1])
        JavaPairRDD<String, Iterable<Integer>> grouprdd = pairRDD.groupByKey();

        //将value累加
        grouprdd.mapValues(integers -> {
                    int sum =0;
                    for (Integer eulm:integers){
                        sum += eulm;
                    }
                    return sum;
                }).collect()
                .forEach(System.out::println);
        //创建rdd(javaRDD或JavaPairRDD)


        //关闭资源
        jsc.stop();
    }
}
