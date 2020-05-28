package com.kevin.java.sparksql_sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author kevin
 * @version 1.0
 * @description     在SparkStreaming中使用SparkSql查询
 * @createDate 2019/1/20
 */
public class SQLWithStreaming {

    public static void main(String[] args) {

        // 1.创建sparkconf设置作业名称和模式，使用本地模式，最少需要两个线程，一个接收数据，一个处理数据
        SparkConf conf = new SparkConf().setAppName("SQLWithStreaming").setMaster("local[2]");
        // 2.设置流计算入口，接收时间为5秒
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // 3.设置checkpoint，作为持久化和容错
        String fileName = "./checkpoint";
        jsc.checkpoint(fileName);

        // 4.数据源为socket，设置节点和端口
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("Master", 9999);

        // 5.每五秒获取socket中的数据然后用空格切分成多个字符串并返回
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        // 6.设置时间窗口函数，每15秒执行一次最近60分钟内的数据
        JavaDStream<String> windowWords = words.window(Durations.minutes(60), Durations.seconds(15));

        // 7.遍历窗口中的数据
        windowWords.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> javaRDD) throws Exception {
                // 8.将数据创建的到每一行中
                JavaRDD<Row> rowJavaRDD = javaRDD.map(new Function<String, Row>() {
                    @Override
                    public Row call(String s) throws Exception {
                        return RowFactory.create(s);
                    }
                });
                // 9.根据使用rdd获取到context并创建SQLContext用于操作sparksql
                SQLContext sqlContext = SQLContext.getOrCreate(javaRDD.context());
                // 10.创建schema对应的字段名称和类型的类型
                ArrayList<StructField> fields = new ArrayList<>();
                // 11.往集合中添加创建的字段名称和类型
                fields.add(DataTypes.createStructField("word", DataTypes.StringType, true));
                // 12.将集合中的字段创建成StructType
                StructType createStructType = DataTypes.createStructType(fields);
                // 13.将数据集和字段创建到DataFrame中
                DataFrame df = sqlContext.createDataFrame(rowJavaRDD, createStructType);
                // 14.将其映射成一张临时表
                df.registerTempTable("words");
                // 15.使用sql查询该表的数据
                DataFrame result = sqlContext.sql("select word, count(*) rank from words group by word order by rank");
                // 16.显示前1000条的数据
                result.show(1000);
            }
        });

        // 17.启动sparkstreaming
        jsc.start();
        // 18.等待被终止
        jsc.awaitTermination();
        // 19.关闭sparkstreaming
        jsc.close();

    }
}
