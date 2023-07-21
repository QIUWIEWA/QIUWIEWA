package com.stark.spark.InputOrOutput;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.parquet.example.Paper.schema;

public class Parquetfile {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("ParquetFileExample").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        // 从 Parquet 文件读取数据
        JavaRDD<Row> rdd = sparkSession.read().parquet("path/to/parquetfile.parquet").javaRDD();

        // 处理数据
        // ...

        // 将数据保存为 Parquet 文件
//        Dataset<Row> dataset = sparkSession.createDataFrame(rdd, schema);
//        dataset.write().parquet("path/to/output.parquet");

        sparkContext.close();
    }
}