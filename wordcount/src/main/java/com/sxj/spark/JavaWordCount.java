package com.sxj.spark;

import com.sxj.util.FileUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple1;
import scala.Tuple2;

import javax.management.relation.RelationSupport;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author sxj
 * @date 2018-09-04 21:14
 */
public final class JavaWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args){
        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }
/*
${BASE_DIR}/spark-1.3.0-bin-hadoop2.4/bin/spark-submit --master yarn-client --num-executors 1 --executor-memory 1g --executor-cores 1 --driver-memory 1g --class com.sxj.spark.JavaWordCount  /home/bsauser/sxj/wordcount-1.0-jar-with-dependencies.jar /apps/bsa_mlengine/store/dns_tunnel/public_suffix_list.txt

 */
        SparkConf sparkConf = new SparkConf()
                .setAppName("JavaSparkDemo");
//                .set("spark.ui.port", "4045");

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
//        创建完session之后返回到主函数继续执行。
        JavaRDD<String> lines = ctx.textFile(args[0], 5);
//        读取每一行数据进行切分，返回一个新的RDD
        JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(SPACE.split(s)));

        //读取每一行数据进行切分，返回一个新的RDD
        JavaPairRDD<String, Integer> ones = words.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));

        //对相同key的单词进行求和计算
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2);

        List<Tuple2<String, Integer>> output = counts.collect();

        String results = "";

        for (Tuple2<String, Integer> tuple : output) {
            String result = tuple._1 + ": " + tuple._2;
            System.out.println(tuple._1 + ": " + tuple._2);
            if(result != null && result != ""){
                results = results + result;
            }
        }

        if (results != "") {
            FileUtil.WriteTxtFile("./javawordCount.txt", results);
        }

        ctx.stop();

    }
}