package com.sxj.spark;

import org.apache.commons.collections.iterators.ArrayListIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author sxj
 * @date 2018-09-05 12:38
 */
public class LocalWordCount {
    public static void main(String[] args) {
        /*
         第一步：创建spark的配置对象sparkconf,设置spark程序的运行时的配置信息，例如说通过setMaster来设置程序
         链接spark集群的master的URL，如果设置为local，则代表spark程序在本地运行，
         */
        SparkConf conf = new SparkConf().setAppName("LocalWordCount");
        //.setMaster("local")
        /*
            第二步：创建JavaSparkContext对象，
            JavaSparkContext是spark程序所有功能的唯一入口，无论是采用Scala，Java，Python，R等都必须有一个JavaSparkContext
            JavaSparkContext核心作用：初始化spark应用程序运行时候所需要的核心组件，包括DAGScheduler，TaskScheduler,SchedulerBackend
            同时还会负责Spark程序往Master注册程序等
                    JavaSparkContext是整个spark应用程序中最为至关重要的一个对象
         */
        JavaSparkContext sc = new JavaSparkContext(conf);
        /*
        第3步：根据具体的数据来源 (HDFS,HBase,Local等)通过JavaSparkContext来创建JavaRDD
        JavaRDD的创建有3种方式，外部的数据来源，根据scala集合，由其他的RDD操作
        数据会被JavaRDD划分成为一系列的Partitions,分配到每个Partition的数据属于一个Task的处理范畴
         */
        JavaRDD<String> lines = sc.textFile("I:\\sxj\\BSA\\tomcat-catalina.log.2018-06-11", 1);
          /*
        第4步：对初始的JavaRDD进行Transformation级别的处理，例如Map、filter等高阶函数等的编程来进行具体的数据计算
         在对每一行的字符串拆分成单个单词
         在单词的拆分的基础上对每个单词实例计算为1，也就是word=>(word,1)
         在对每个单词实例计数为1基础上统计每个单词在文件中出现的总次数
         */
        JavaRDD<String> words= lines.flatMap(line -> Arrays.asList(" ".split(line)));

        JavaPairRDD<String, Integer> pairs = words.mapToPair(word ->new Tuple2<>(word, 1));

        JavaPairRDD<String, Integer> wordscount = pairs.reduceByKey((v1, v2) -> v1 + v2);

        List<Tuple2<String, Integer>> collects = wordscount.collect();
        for (Tuple2<String, Integer> collect : collects) {
            System.out.println(collect._1() + ": " + collect._2());
        }
//        sc.stop();
    }
}
