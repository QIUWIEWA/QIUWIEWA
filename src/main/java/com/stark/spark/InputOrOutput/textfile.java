package com.stark.spark.InputOrOutput;
/**
 *  saveAsTextFile(path)：将 RDD 中的元素保存到指定路径的文本文件中。
 *  saveAsObjectFile(path)：将 RDD 中的元素保存为序列化的 Java 对象文件。
 *  saveAsParquetFile(path)：将 DataFrame 保存为 Parquet 格式文件。*/
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class textfile {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("TextFileExample").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // 从文本文件读取数据
        JavaRDD<String> rdd = sparkContext.textFile("path/to/textfile.txt");

        // 处理数据
        // ...
        rdd.saveAsTextFile("path/to/output");
        sparkContext.close();
    }
}
