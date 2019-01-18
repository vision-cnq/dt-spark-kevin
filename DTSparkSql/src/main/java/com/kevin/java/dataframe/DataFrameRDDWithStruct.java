package com.kevin.java.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

/**
 * @author kevin
 * @version 1.0
 * @description     动态创建Scheme将非json格式RDD转换成DataFrame，推荐
 * @createDate 2019/1/6
 */
public class DataFrameRDDWithStruct {

    public static void main(String[] args) {

        // 1.创建SparkConf作业设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("DataFrameRDDWithStruct").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.根据sc创建SQLContext对象对sql进行分析处理
        SQLContext sqlContext = new SQLContext(sc);

        // 4.读取文件数据
        String file = "DTSparkSql\\src\\main\\resources\\person.txt";
        JavaRDD<String> lineRDD = sc.textFile(file);

        // 5.转成Row类型的RDD
        JavaRDD<Row> rowJavaRDD = lineRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {
                String[] s = line.split(",");
                return RowFactory.create(s[0], s[1], Integer.valueOf(s[2]));
            }
        });

        // 6.动态构建DataFrame中的元数据，一般来说这里的字段可以源于字符串，也可以源于外部数据库
        List<StructField> structFields = Arrays.asList(
                DataTypes.createStructField("id", DataTypes.StringType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true));

        // 7.将list转为DataFrame中的元数据
        StructType structType = DataTypes.createStructType(structFields);

        // 8.将row类型的rdd数据和对应的字段名称类型转成DataFrame
        DataFrame df = sqlContext.createDataFrame(rowJavaRDD, structType);

        // 9.查询前20行数据
        df.show();

        // 10.将DataFrame转为RDD
        JavaRDD<Row> javaRDD = df.javaRDD();

        // 11.遍历rdd数据
        javaRDD.foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                System.out.println((String)row.getAs("name"));
            }
        });

        // 12.关闭
        sc.close();

    }
}
