package com.stark.spark.Rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * ---distinct:去重----
 * 简介
 * 去重全部元素相同的行，如果需要去重部分列相同的使用groupBy
 * 他需要落盘，且可以指定分区个数
 * ---
 * 实例：
 * [1,2,3,1,2,1,3,1,3,2,1]    --distinct-->  [1,2,3]
 * --
 * 代码：
 * javaRDD.distinct(3)
 * */
public class use06_distinct {
    public static void main(String[] args) {
        //创建conf配置参数
        SparkConf sparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[2]");

        //创建spark运行环境
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //编写代码
        List<Integer> list = Arrays.asList(1, 2, 4, 5, 7, 8, 1, 2, 4, 5);
        JavaRDD<Integer> javaRDD = jsc.parallelize(list,2);

//        distinct:去重，需要落盘一次(参数为指定分区数)
        javaRDD.distinct(3).collect().forEach(System.out::println);


        System.out.println("***************用groupBy去重**************");
        javaRDD.groupBy(integer ->integer).map(integerIterableTuple2 -> integerIterableTuple2._1).collect().forEach(System.out::println);
        //关闭资源
        jsc.stop();
    }
}
