package com.stark.spark.PairRdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
/**
 * ----reduceByKey----
 * 按照key进行分组
 * 同时我们可以指定对同一组的values进行操作
 * */
public class use06_reduceByKey {
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
        JavaPairRDD<String, Integer> pairRDD = (JavaPairRDD<String, Integer>) jsc.parallelizePairs(list);


        //相比于use07中的，先group分组之后在相加，速度要快百倍（因为会在落盘之前先进行预聚合，即每个分区都会先reduce一次，之后对所有分区在总的一次reduce）
        //第一个参数：sum
        //第二个参数：emle（每个key对应的value）
        //输出：sum + emle
        pairRDD.reduceByKey((sum,emle) -> sum+emle)
                .collect()
                .forEach(System.out::println);

        //关闭资源
        jsc.stop();
    }
}
