package com.stark.spark.cache;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;


/**
 * 当血缘关系过长时，内存缓存丢失重新计算会得不偿失，
 * 故checkpoint是已二进制写入到文件夹里面，可以在21行通过如下方法定义存储路径
 * .set("spark.checkpoint.dir", "path/to/checkpoint")
 * 使用checkpoint的数据，会剪断血缘关系
 * */
public class checkpoint {
    public static void main(String[] args) {
        //创建conf配置参数
        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkCore")
                .setMaster("local[2]");

        //创建spark运行环境
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        jsc.setCheckpointDir("ck");

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

//        当血缘关系过长时，内存缓存丢失重新计算会得不偿失，
//        故checkpoint是已二进制写入到文件夹里面，可以在21行定义存储路径
//        使用checkpoint的数据，会剪断血缘关系
        pairRDD.checkpoint();


        JavaPairRDD<String, Integer> reduceRDD = pairRDD.reduceByKey((sum, elem) -> sum + elem);
        JavaPairRDD<String, Integer> reduceRDD1 = pairRDD.reduceByKey((sum, elem) -> sum + elem);

        //关闭资源
        jsc.stop();
    }
}
