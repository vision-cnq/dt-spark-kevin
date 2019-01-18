package com.kevin.java.dataframe;

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
 * @description     通过反射的方式将非json格式的RDD转换成DataFrame，不推荐使用
 * @createDate 2019/1/6
 */
public class DataFrameRDDWithReflect {

    public static void main(String[] args) {

        // 1.创建SparkConf作业设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("DataFrameRDDWithReflect").setMaster("local");

        // 2.基于Sparkconf对象创建一个SparkContext上下文，它是通往集群的唯一通道，且在创建时会创建任务调度器
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3.根据sc创建SQLContext对象对sql进行分析处理
        SQLContext sqlContext = new SQLContext(sc);

        // 4.读取文件数据
        String file = "DTSparkSql\\src\\main\\resources\\person.txt";
        JavaRDD<String> lineRDD = sc.textFile(file);

        // 5.获取每一行txt数据转为person类型
        JavaRDD<Person> personRDD = lineRDD.map(new Function<String, Person>() {
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

        // 6.传入Person的时候，sqlContext是通过反射的方式创建DataFrame
        // 在底层通过反射的方式获得Person的所有field，结合RDD本，就生成了DataFrame
        DataFrame df = sqlContext.createDataFrame(personRDD, Person.class);

        // 7.将DataFrame注册成表
        df.registerTempTable("person");

        // 8.查询id=2的数据
        DataFrame sql = sqlContext.sql("select id,name,age from person where id=2");
        sql.show();

        // 9.将DataFrame转成RDD并用row.getAs("列名")获取数据
        JavaRDD<Row> javaRDD = df.javaRDD();
        JavaRDD<Person> map = javaRDD.map(new Function<Row, Person>() {
            @Override
            public Person call(Row row) throws Exception {
                Person p = new Person();
                p.setId((String) row.getAs("id"));
                p.setName((String) row.getAs("name"));
                p.setAge((Integer) row.getAs("age"));
                return p;
            }
        });

        // 10.遍历数据
        map.foreach(new VoidFunction<Person>() {
            @Override
            public void call(Person person) throws Exception {
                System.out.println(person);
            }
        });

        // 11.关闭
        sc.close();


    }
}
