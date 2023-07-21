package com.stark.spark.ActionRdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;


/**
 * collect
 * 行动算子
 * 将Execute中的数据拉取到driver中。然后单线程输出，性能低下，但有顺序
 * 若数据量未经筛选，可能会有oom的风险，慎用
 * --
 * 与直接使用foreach相比，
 * 直接使用foreach是多线程输出，但无序
* */
public class use_collect {
    public static void main(String[] args) {
        //创建spark配置文件
        SparkConf sparkConf= new SparkConf().setMaster("local[2]").setAppName("sparkcore");

        //创建spark运行环境
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);


        //代码编写区
        //创建列表，作为spark初始rdd的数据
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> javaRDD = jsc.parallelize(list);

        javaRDD.collect()
                .forEach(System.out::println);


        //关闭资源
//    jsc.stop();
    }


}
