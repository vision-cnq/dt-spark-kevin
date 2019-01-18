package com.kevin.java.udf_udaf;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author kevin
 * @version 1.0
 * @description     UDF用户自定义函数
 * 例子：需求：使用自定义UDF获取name的长度
 * @createDate 2019/1/12
 */
public class UDF {

    public static void main(String[] args) {

        // 1.创建SparkConf作业设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("UDF").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.创建sqlContext操作sparksql
        SQLContext sqlContext = new SQLContext(sc);

        // 4.创建list数据，转成rdd
        JavaRDD<String> parallelize = sc.parallelize(Arrays.asList("zhangsan", "lisi", "wangwu", "maliu"));

        // 5.将RDD数据转成row，每一行row都只有一个数据
        JavaRDD<Row> rowRDD = parallelize.map(new Function<String, Row>() {
            // 每进来一个数据都作为一行数据
            @Override
            public Row call(String s) throws Exception {
                return RowFactory.create(s);
            }
        });

        // 6.创建一个StructField类型的集合
        List<StructField> fields = new ArrayList<StructField>();

        // 7.动态创建Schema方式加载DataFrame，叫做name
        fields.add(DataTypes.createStructField("name",DataTypes.StringType,true));

        // 8.将list转为DataFrame中的元数据
        StructType schema = DataTypes.createStructType(fields);

        // 9.将row类型的rdd数据和对应的字段名称类型转成DataFrame
        DataFrame df = sqlContext.createDataFrame(rowRDD, schema);

        // 10.将数据注册成一张临时表
        df.registerTempTable("user");

        // 11.根据UDF函数参数的个数来决定是实现哪一个UDF ，UDF1,UDF2....
        sqlContext.udf().register("StrLen", new UDF1<String, Integer>() {
            // 参数1：UDF函数的名称，参数2：传入的数据类型，参数3：传出的数据类型
            @Override
            public Integer call(String s) throws Exception {
                // 返回传入数据的长度
                return s.length();
            }
        },DataTypes.IntegerType);

        // UDF多加一个参数，对UDF再进一步处理
        /*sqlContext.udf().register("StrLen", new UDF2<String, Integer, Integer>() {

            @Override
            public Integer call(String s, Integer i) throws Exception {
                return s.length()+i;
            }
        },DataTypes.IntegerType);*/

       // 12.查询sql语句，使用自定义UDF获取name的长度
       sqlContext.sql("select name,StrLen(name,10) as length from user").show();

       // 13.关闭sc
       sc.stop();

    }
}
