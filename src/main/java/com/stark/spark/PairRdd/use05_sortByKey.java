package com.stark.spark.PairRdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;


public class use05_sortByKey {
    public static void main(String[] args) {
        //创建conf配置参数
        SparkConf sparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[2]");

        //创建spark运行环境
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //编写代码

        ArrayList<Tuple2<Integer,Integer>> list= new ArrayList<>();
        list.add(new Tuple2<>(2,1));
        list.add(new Tuple2<>(1,2));
        list.add(new Tuple2<>(4,5));
        list.add(new Tuple2<>(7,4));
        list.add(new Tuple2<>(3,3));
        list.add(new Tuple2<>(5,6));
        list.add(new Tuple2<>(6,9));
        list.add(new Tuple2<>(8,8));
        list.add(new Tuple2<>(9,7));
        JavaPairRDD<Integer, Integer> pairRDD = (JavaPairRDD<Integer, Integer>)jsc.parallelizePairs(list);

//        即按照key进行排序
        pairRDD.sortByKey()
                .collect()
                .forEach(System.out::println);

//        按照values排序,方案1，将pairRDD转换成RDD，在使用sortBy
        pairRDD.map(v1 ->v1)
                .sortBy(v1 ->v1._2,true,1);

//        按照values排序，方案2，将key和values换位置，排完序再换回来
        pairRDD.mapToPair(v1 ->new Tuple2<>(v1._2,v1._1))
                .sortByKey()
                .mapToPair(v1 ->new Tuple2<>(v1._2,v1._1));

        //关闭资源
        jsc.stop();
    }
}
