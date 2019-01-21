package com.kevin.java.sparksql_sparkstreaming;

import com.kevin.java.entity.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @author kevin
 * @version 1.0
 * @description     使用创建自定义类型的方式使用sql
 * @createDate 2019/1/20
 */
public class SQLWithStreaming2 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("SQLWithStreaming2").setMaster("local[2]");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        String fileName = "./checkpoint";
        jsc.checkpoint(fileName);

        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("Master", 9999);

        JavaDStream<String> window = lines.window(Durations.minutes(60), Durations.seconds(15));

        JavaDStream<Person> personDS = window.map(new Function<String, Person>() {
            @Override
            public Person call(String s) throws Exception {
                String[] split = s.split(" ");
                Person person = new Person(Integer.valueOf(split[0]), split[1], split[2], Integer.valueOf(split[3]));
                return person;
            }
        });

        personDS.foreachRDD(new VoidFunction<JavaRDD<Person>>() {
            @Override
            public void call(JavaRDD<Person> personJavaRDD) throws Exception {
                SQLContext sqlContext = SQLContext.getOrCreate(personJavaRDD.context());
                DataFrame df = sqlContext.createDataFrame(personJavaRDD, Person.class);
                df.registerTempTable("person");
                sqlContext.sql("select id, name, gender, age from person").show();

            }
        });

        jsc.start();
        jsc.awaitTermination();
        jsc.close();

    }
}
