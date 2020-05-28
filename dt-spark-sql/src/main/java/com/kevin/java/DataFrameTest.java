package com.kevin.java;

import com.kevin.java.entity.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * @author kevin
 * @version 1.0
 * @description     读取文件数据转为表形式查询
 * @createDate 2019/1/6
 */
public class DataFrameTest {

    public static void main(String[] args) {

        // 1.创建SparkConf作业设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("DataFrameTest").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.根据sc创建SQLContext上下文对象用于SQL的分析
        SQLContext sqlContext = new SQLContext(sc);

        // 4.读取文件数据
        String file = "DTSparkSql\\src\\main\\resources\\person.txt";
        JavaRDD<String> lineRDD = sc.textFile(file);

        // 5.遍历每一行数据封装到Person类型中
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

        // 6.传入进去Person.class的时候，sqlContext是通过反射的方式创建DataFrame
        // 在底层通过反射的方式获得Person的所有field，结合RDD本身，便生成了DataFrame
        DataFrame df = sqlContext.createDataFrame(map, Person.class);

        // 7.以数据框形式查看表中的数据，类似：select * from person
        df.show();
        // 8.查看Schema结构数据
        df.printSchema();
        // 9.注册成临时表
        df.registerTempTable("person");
        // 10.查询自定义的sql语句的数据
        DataFrame sql = sqlContext.sql("select id,name,age from person where id=2");

        // 11.将DataFrame转为RDD形式输出
        JavaRDD<Row> javaRDD = sql.javaRDD();

        // 12.自己写的sql语句查询出来的DataFrame显示表的时候会安照查询的字段来显示。
        javaRDD.foreach(new VoidFunction<Row>() {

            // 可以使用row.getAs("列名")来获取对应的值，推荐
            // 可以使用row.getAs(0),与row.getString(0)等通过下标获取返回Row类型的数据，需要注意顺序，不推荐
            @Override
            public void call(Row row) throws Exception {
                System.out.println("根据字段名称获取数据，推荐...");
                System.out.println("id = "+ row.getAs("id"));
                System.out.println("name = "+ row.getAs("name"));
                System.out.println("age = "+ row.getAs("age"));
                /*System.out.println("根据下标获取数据，不推荐...");
                System.out.println("name = "+ row.getAs(0));
                System.out.println("age = "+ row.getAs(1));
                System.out.println("id = "+ row.getAs(2));
                System.out.println("根据下标和数据类型获取数据，不推荐...");
                System.out.println("name = "+ row.getString(0));
                System.out.println("age = "+ row.getInt(1));
                System.out.println("id = "+ row.getString(2));*/
            }
        });

        // 13.关闭
        sc.close();
    }
}
