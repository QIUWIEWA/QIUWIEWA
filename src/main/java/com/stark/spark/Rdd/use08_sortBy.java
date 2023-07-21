package com.stark.spark.Rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;


/**
 * ---sortBy---
 * 简介：
 * 对数据进行排序，
 * ---
 * 参数：
 * 【对哪个值进行排序，正序 or 倒序，分几个区】
 * ---
 * 排序规则：
 *  1，抽样：抽取n个样本
 *  2，对样本排序
 *  3，取样本最大值和最小值eg：1,100
 *  4，通过最大最小值进行分区，eg：需要三个分区 （负无穷，33,] [34,66] [67,正无穷)
 *  5，需要输出时按分区号
 * */
public class use08_sortBy {
    public static void main(String[] args) {
        //创建conf配置参数
        SparkConf sparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[2]");

        //创建spark运行环境
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //编写代码
        List<Integer> list = Arrays.asList(1, 5,6,7,5,48,12,5,45,12,34,56);


        //创建rdd(javaRDD或JavaPairRDD)
        JavaRDD<Integer> javaRDD = jsc.parallelize(list,2);

//        sortby(函数筛选需要对哪个值排序，正序还是倒序，分几个区)
        javaRDD.sortBy(integer -> integer,true,3);

        //关闭资源
        jsc.stop();
    }
}
