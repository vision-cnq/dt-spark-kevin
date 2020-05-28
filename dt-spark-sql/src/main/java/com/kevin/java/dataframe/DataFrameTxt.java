package com.kevin.java.dataframe;

import com.kevin.java.entity.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * @author kevin
 * @version 1.0
 * @description     读取txt文件转成DataFrame形式操作
 * @createDate 2019/1/6
 */
public class DataFrameTxt {

    public static void main(String[] args) {
        // 1.创建SparkConf并设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("DataFrameTxt").setMaster("local");

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

        // 8.关闭sc
        sc.close();

    }
}
