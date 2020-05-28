package com.kevin.java.secondsort;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * @author kevin
 * @version 1.0
 * @description     二次排序
 * @createDate 2019/1/2
 */
public class SecondarySort {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("SecondarySort").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String file = "DTSparkCore\\src\\main\\resources\\secondSort.txt";
        JavaRDD<String> lines = sc.textFile(file);

        // 将数据封装成SecondSortKey类型
        JavaPairRDD<SecondSortKey, String> pairRDD = lines.mapToPair(new PairFunction<String, SecondSortKey, String>() {
            @Override
            public Tuple2<SecondSortKey, String> call(String line) throws Exception {
                String[] splited = line.split(" ");
                int first = Integer.valueOf(splited[0]);
                int second = Integer.valueOf(splited[1]);
                SecondSortKey secondSortKey = new SecondSortKey(first, second);
                return new Tuple2<SecondSortKey, String>(secondSortKey, line);
            }
        });
        // 重写排序方法，在使用排序的时候应用其自定义的排序方法
        pairRDD.sortByKey(false).foreach(new VoidFunction<Tuple2<SecondSortKey, String>>() {
            @Override
            public void call(Tuple2<SecondSortKey, String> tuple2) throws Exception {
                System.out.println(tuple2._2);
            }
        });

        sc.close();

    }
}
