package com.kevin.java.dataframe;

import com.kevin.java.entity.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * @author kevin
 * @version 1.0
 * @description     读取txt文件转为DataFrame表的所有action
 * @createDate 2019/1/6
 */
public class DataFrameActions {

    public static void main(String[] args) {

        // 1.创建SparkConf并设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("DataFrameActions").setMaster("local");

        // 2.基于sparkConf创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.创建SQLContext对象对sql进行分析处理
        SQLContext sqlContext = new SQLContext(sc);

        // 4.读取文件数据
        String file = "DTSparkSql\\src\\main\\resources\\person.txt";
        JavaRDD<String> lineRDD = sc.textFile(file);

        // 5.获取每一行txt数据转为person类型
        JavaRDD<Person> map = lineRDD.map(new Function<String, Person>() {
            @Override
            public Person call(String line) throws Exception {
                String[] s = line.split(",");
                Person p = new Person();
                p.setId(s[0]);
                p.setName(s[1]);
                p.setAge(Integer.valueOf(s[2]));
                return p;
            }
        });

        // 6.将person类型的数据转为DataFrame表
        DataFrame df = sqlContext.createDataFrame(map, Person.class);

        // 7.查看所有数据，默认前20行
        df.show();

        // 8.查看前n行数据
        df.show(2);

        // 9.是否最多只显示20个字符，默认为true
        df.show(true);
        df.show(false);

        // 10.显示前n行数据，以及对过长字符串显示格式
        df.show(2,false);

        // 11.打印schema
        df.printSchema();

        // 12.获取所有数据转到数据中
        Row[] collect = df.collect();
        for (Row r : collect) {
            Object name = r.getAs("name");
            System.out.println("数据中name: " + name);
        }

        // 13.获取所有数据转到集合中
        List<Row> rows = df.collectAsList();
        for (Row r : rows) {
            Object name = r.getAs("name");
            System.out.println("集合中name: " + name);
        }

        // 14.获取指定字段的统计信息
        df.describe("name").show();

        // 15.获取第一行记录
        Row first = df.first();
        Object firstName = first.getAs("name");
        System.out.println("获取第一行记录中的name: "+firstName);

        // 16.head()获取第一行记录，head(n)获取前n行记录，下标从1开始
        Row[] head = df.head(2);
        for (Row r : head) {
            Object name = r.getAs("name");
            System.out.println("head中name: " + name);
        }

        // 使用take和takeAsList会将获得到的数据返回到Driver端，所以，使用这两个方法的时候需要少量数据
        // 17.获取前n行数据，下标从1开始
        Row[] take = df.take(2);
        for (Row r : take) {
            Object name = r.getAs("name");
            System.out.println("take中name: " + name);
        }

        // 18.获取前n行记录，以list形式展现
        List<Row> takeAsList = df.takeAsList(2);
        for(Row takes : takeAsList) {
            Object name = takes.getAs("name");
            System.out.println("takeAsList中name: " + name);
        }

        // 19.关闭
        sc.close();

    }
}
