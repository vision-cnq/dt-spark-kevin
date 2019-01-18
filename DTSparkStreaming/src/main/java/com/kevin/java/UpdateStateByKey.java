package com.kevin.java;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author kevin
 * @version 1.0
 * @description     增量更新UpdateStateByKey
 * 主要功能
 * 1.为sparkstreaming中每一个key维护一份state状态，state类型可以是任意的，可以是一个自定义的对象，那么更新函数也可以是自定义的。
 * 2.通过更新函数对该key的状态不断更新，对于每个新的batch而已，sparkstreaming会在使用upDateStateByKey的时候为已经存在的key进行state的状态更新
 *
 * 如果要不断的更新每个key的state，就一定涉及到了状态的保存和容错，这个时候就需要开启checkpoint机制和功能
 *
 * 统计广告点击流量，统计这一天的车流量，统计点击量
 *
 * @createDate 2019/1/17
 */
public class UpdateStateByKey {

    public static void main(String[] args) {

        // 1.创建sparkconf并设置作业名称和模式
        SparkConf conf = new SparkConf().setAppName("UpdateStateByKey").setMaster("local[2]");

        // 2.基于sparkconf创建JavaStreamingContext，设置接收数据间隔为5秒
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        String checkpointDirectory = "hdfs://Master:9000/sparkstreaming/CheckPointState";
        /**
         * 设置checkpoint目录
         * 多久会将内存中的数据（每一个key所对应的状态）写入到磁盘上一份呢？
         * 呢，那么10s会将内存中的数据写入磁盘一份
         * 如果batchInterval大于10s，那么久以batchInterval为准
         * 这样是为了防止频繁的写HDFS
         */
        jsc.checkpoint(checkpointDirectory);

        // 3.将socket作为数据源，配置节点和端口
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("Master", 9999);

        // 4.获取socket端口的数据并且切分成单词
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        // 5.每个单词初始次数为1
        JavaPairDStream<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        //
        JavaPairDStream<String, Integer> counts = ones.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

            // 对相同的key，进行value的累计（包括local和reducer级别的reduce）
           @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                /**
                 * values：经过分组后最后这个key所对应的value[1,1,1,,]
                 * state：这个key在本次之前之前的状态
                 */
                Integer updateValue = 0;
                if (state.isPresent()) {
                    updateValue = state.get();
                }
                for (Integer value : values) {
                    updateValue += value;
                }
                return Optional.of(updateValue);
            }
        });

        // 打印结果数据
        counts.print();

        // 启动SparkStreaming
        jsc.start();
        // 等待被终止
        jsc.awaitTermination();
        // 关闭SparkStremaing
        jsc.close();


    }
}
