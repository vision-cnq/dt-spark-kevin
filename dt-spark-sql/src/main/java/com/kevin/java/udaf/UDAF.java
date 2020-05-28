package com.kevin.java.udaf;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author kevin
 * @version 1.0
 * @description     UDAF用户自定义聚合函数
 * 例子：需求：实现统计相同name值的个数
 * @createDate 2019/1/12
 */
public class UDAF {

    public static void main(String[] args) {

        // 1.创建SparkConf作业设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("UDAF").setMaster("local");
        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 3.创建sqlContext操作sparksql
        SQLContext sqlContext = new SQLContext(sc);
        // 4.自定义数据作为测试
        JavaRDD<String> parallelize = sc.parallelize(Arrays.asList("zhangsan", "lisi", "wangwu", "maliu","zhangsan","zhangsan","lisi"));

        // 5.获取每一条数据转为每一行Row，以便动态转为表
        JavaRDD<Row> rowRDD = parallelize.map(new Function<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                return RowFactory.create(s);
            }
        });

        // 6.创建StrutructField类型的list
        ArrayList<StructField> fields = new ArrayList<>();
        // 7.设置字段名称和类型
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        // 8.将表字段和类型设置到schma中
        StructType schema = DataTypes.createStructType(fields);
        // 9.获取每一行数据及对应的字段和类型，转成DataFrame
        DataFrame df = sqlContext.createDataFrame(rowRDD, schema);
        // 10.将DataFrame注册为临时表
        df.registerTempTable("user");

        // 11.注册一个UDAF函数，实现统计相同值的个数
        // 注意:这里可以自定义一个类集成UserDefinedaggregateFunction类也是可以的
        sqlContext.udf().register("StringCount", new UserDefinedAggregateFunction() {

            // 初始化一个内部的自己定义的值，在Aggregate之前每组数据的初始化结果
            @Override
            public void initialize(MutableAggregationBuffer buffer) {
                buffer.update(0,0);
            }

            /**
             * 更新
             * 可以认为一个一个地将组内的字段值传递进来，实现拼接的逻辑
             * buffer.getInt(0)获取的是上一次聚合后的值
             * 相当于map端的combiner，combiner就是对每一个maptask的处理结果进行一次小聚合
             * 大聚合发生在reduce端
             * 这里是：在进行聚合的时候，每当有新的值进来，对分组后的聚合 如何进行计算
             * @param buffer
             * @param input
             */
            @Override
            public void update(MutableAggregationBuffer buffer, Row input) {
                buffer.update(0,buffer.getInt(0)+1);
            }

            /**
             * 合并
             * update操作，可能是针对一个分组内的部分数据，在某个节点上发生的，但是一个分组内的数据，
             * 会分布在多个节点上处理
             *
             * 此时就要使用merge操作，将各个节点上分布式拼接好的串，合并起来
             * buffer1.getInt(0)：大聚合的时候，上一次聚合后的值
             * buffer2.getInt(0)：这次计算传入进来的update的结果
             *
             * 这里就是：最后再分布式节点完成后需要进行全局级别的Merge操作
             * @param buffer1
             * @param buffer2
             */
            @Override
            public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
                buffer1.update(0,buffer1.getInt(0)+buffer2.getInt(0));
            }

            // 在进行聚合操作的时候所要处理的数据的结果的类型
            @Override
            public StructType bufferSchema() {
                return DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("bffer",DataTypes.IntegerType,true)));
            }

            // 最后返回一个和dataType方法的类型要一致的类型，返回UDAFzu最后的计算结果
            @Override
            public Object evaluate(Row row) {
                return row.getInt(0);
            }

            // 指定UDAF函数计算后返回的结果类型
            @Override
            public DataType dataType() {
                return DataTypes.IntegerType;
            }

            // 指定输入字段的字段及类型
            @Override
            public StructType inputSchema() {
                return DataTypes.createStructType(Arrays.asList(
                        DataTypes.createStructField("namexxx",DataTypes.StringType,true)));
            }

            // 确保一致性，一般用true，用以标记针对给定的一组输入，UDAF是否总是生成相同的结果
            @Override
            public boolean deterministic() {
                return true;
            }

        });

        // 12.使用自定的UDAF函数统计相同值的个数
        sqlContext.sql("select name,StringCount(name) as strCount from user group by name").show();
        // 13.关闭sc
        sc.stop();
    }
}
