package com.sjzy.application;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.alibaba.fastjson.JSONObject;
import com.sjzy.common.CommonConstants;
import com.sjzy.conf.KafkaConf;
import com.sjzy.processor.Calculation;
import com.sjzy.utils.MysqlUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.sjzy.utils.MysqlUtil.getTablePK;
import static com.sjzy.utils.MysqlUtil.test;


/**
 * Created by wpp on 2020/7/21.
 *
 */
public class Application {

    public static final Logger logger = LoggerFactory.getLogger(Application.class);
    public static Map<String,String> mapTablePk = null;

    public static void main(String[] args) throws InterruptedException {

        //找到mysql中的映射表信息
//        mapTablePk = getTablePK(CommonConstants.MYSQL_MAP_TABLE_NAME);

        /////////////////////////////////////////////////////////////
        mapTablePk = test("kafka.wpp_test");

        //初始化JavaStreamingContext
        JavaStreamingContext jssc = initJavaStringContext();

        JavaInputDStream<ConsumerRecord<String, String>> stream = null;



        //kafka配置信息
        Map<String, Object> kafkaParams = KafkaConf.getKafkaParams();

        List<Map<String,String>> offsetInfo = MysqlUtil.query(CommonConstants.GET_KAFKA_TOPIC_OFFSET_SQL);

//        /*如果offset未被记录，则证明是首次消费，kafka从头消费数据
//         * 如果被记录，则从记录的offset开始消费kafka数据*/
//        if ( offsetInfo.size()==0 ) {
//
            kafkaParams.put("auto.offset.reset", "earliest");

            stream = KafkaUtils.createDirectStream(jssc,LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, String>Subscribe(Arrays.asList(CommonConstants.KAFKA_TOPIC_NAME), kafkaParams)
            );
            logger.info("----------这里是首次消费kafka数据------------");

//        }else {
//            Map<TopicPartition, Long> fromOffsets = new HashMap<>();
//
//            for (Map<String, String> map : offsetInfo) {
//                logger.info("获取mysql记录表最新offset===>> topic:"+map.get("topic")+",partition:"+map.get("partitiona")+",fromoffset:"+map.get("fromoffset"));
//                fromOffsets.put(new TopicPartition(map.get("topic"), Integer.valueOf(map.get("partitiona"))), Long.valueOf(map.get("fromoffset")));
//            }
//
//            stream = KafkaUtils.createDirectStream(jssc,LocationStrategies.PreferConsistent(),
//                    ConsumerStrategies.<String, String>Assign(fromOffsets.keySet(), kafkaParams, fromOffsets)
//            );
//            logger.info("----------这里是从"+offsetInfo.get(0).get("fromoffset")+"消费kafka数据------------");
//        }


        //逻辑处理
        stream.foreachRDD(new SingleFunction());

        stream.print();
        jssc.start();
        jssc.awaitTermination();

    }

    /**
     * 初始化JavaStreamingContext的配置信息
     * @return  JavaStreamingContext配置信息
     */
    private static JavaStreamingContext initJavaStringContext() {
        //配置SparkConf
        SparkConf sparkConf = new SparkConf()
                .setAppName(CommonConstants.APP_NAME)
                .setMaster("local[3]")
                .set("spark.default.parallelism", CommonConstants.SPARK_DEFAULT_PARALLELISM)
                .set("spark.sql.shuffle.partitions",CommonConstants.SPARK_SQL_SHUFFLE_PARTITIONS)
                .set("spark.serializer", CommonConstants.SPARK_SERIALIZER)
                .set("spark.sql.crossJoin.enabled", CommonConstants.SPARK_SQL_CROSSJOIN_ENABLED);

        //创建SparkStreamingContext,每隔n秒，处理一次
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(CommonConstants.SECOND*15));

        return jssc;
    }


    /**
     * foreachRDD的逻辑编写
     * 存储offset位置信息
     */
    private static class SingleFunction implements VoidFunction<JavaRDD<ConsumerRecord<String, String>>> {

        @Override
        public void call(JavaRDD<ConsumerRecord<String, String>> rdd){

            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

            rdd.foreachPartition(new SinglePartitionFunction());

            //存储offset的信息到mysql中
            List<String> strlist = new ArrayList<String>();

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            for (OffsetRange o : offsetRanges) {
                String value = o.fromOffset()+"^"+o.untilOffset()+"^"+sdf.format(new Date())+"^"+o.topic()+"^"+o.partition()+"^ogg";
                strlist.add(value);
            }
            MysqlUtil.execUpdateOffset(strlist);
        }
    }


    /**
     * 获取信息并存储到es中
     */
    private static class SinglePartitionFunction implements VoidFunction<Iterator<ConsumerRecord<String, String>>> {

        @Override
        public void call(Iterator<ConsumerRecord<String, String>> consumerRecordIterator) throws IOException {

            Map<String,List<JSONObject>> allMapInfo = new HashMap<String,List<JSONObject>>();

            while ( consumerRecordIterator.hasNext() ) {

                //单个信息获取
                ConsumerRecord<String, String> singleInfo = consumerRecordIterator.next();
                if ( singleInfo != null ) {

                    //将信息保存到
                    String singleInfoValue = singleInfo.value();
                    Calculation.getMapForTableName(allMapInfo,singleInfoValue);

                }
            }

            if (allMapInfo != null && allMapInfo.size() > 0) {

                //多线程写入es数据库
                Calculation.nomultiThreadHandle(allMapInfo, mapTablePk);

            }
        }
    }

}
