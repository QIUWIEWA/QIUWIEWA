package com.stark.spark.cache;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
/**
 * ---cache---
 * 当一个RDD后面又多个RDD时，即出现了分叉，就可以对分叉点做缓存
 * cache 内存缓存,当 程序运行结束 或者 内存不足时 就可能会导致缓存丢失,当需要再使用时就会通过血缘关系重新计算
 * ---
 * persist:可以自己选择在磁盘或者内存做缓存，或者同时，
 * 可以选择在磁盘和内存中间的占比，例如磁盘75% ， 内存25%
 * cache() 等价于 persist(MEMORY_ONLY)
 * */
public class cache {
    public static void main(String[] args) {
        //创建conf配置参数
        SparkConf sparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[2]");

        //创建spark运行环境
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //编写代码
        JavaRDD<String> JavaRDD = jsc.textFile("input/1.txt");

        //创建rdd(javaRDD或JavaPairRDD)
        JavaRDD<Tuple2<String, Integer>> flatmap = JavaRDD.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();
                String[] s_list = s.split(" ");
                for (String st : s_list) {
                    if (!"".equals(st) && null != st) {
                        list.add(new Tuple2<>(st, 1));
                    }
                }
                return list.iterator();
            }
        });
        JavaPairRDD<String, Integer> pairRDD = flatmap.mapToPair(tuple -> tuple);
//        当一个RDD后面又多个RDD时，即出现了分叉，就可以对分叉点做缓存
//       cache 内存缓存,当 程序运行结束 或者 内存不足时 就可能会导致缓存丢失,当需要再使用时就会通过血缘关系重新计算
        pairRDD.cache();
//        persist:可以自己选择在磁盘或者内存做缓存，或者同时，
        pairRDD.persist(StorageLevel.MEMORY_AND_DISK());


        JavaPairRDD<String, Integer> reduceRDD = pairRDD.reduceByKey((sum, elem) -> sum + elem);
        JavaPairRDD<String, Integer> reduceRDD1 = pairRDD.reduceByKey((sum, elem) -> sum + elem);

        //关闭资源
        jsc.stop();
    }
}
