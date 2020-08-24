package com.sjzy.conf;

import com.sjzy.common.CommonConstants;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by wpp on 2020/7/21.
 *
 * 得到kafka相关配置文件信息函数
 */
public class KafkaConf {

    /**
     * 得到 kafkaParams （kafka相关配置）
     *
     * @return  kafkaParams
     */
    public static Map<String, Object> getKafkaParams(){

        Map<String, Object> kafkaParams = new HashMap<>();
        //Kafka服务监听端口
        kafkaParams.put("bootstrap.servers", CommonConstants.KAFKA_BOOTSTRAP_SERVERS);
        //指定kafka输出key的数据类型及编码格式（默认为字符串类型编码格式为uft-8）
        kafkaParams.put("key.deserializer", CommonConstants.KEY_DESERIALIZER);
        //指定kafka输出value的数据类型及编码格式（默认为字符串类型编码格式为uft-8）
        kafkaParams.put("value.deserializer", CommonConstants.VALUE_DESERIALIZER);
        //消费者ID，随意指定
        kafkaParams.put("group.id", CommonConstants.KAFKA_GROUP_ID);//最早运行的是testpoc01，测试临时表用了testpoc02，
        //指定从latest(最新,其他版本的是largest这里不行)还是smallest(最早)处开始读取数据
        //kafkaParams.put("auto.offset.reset", "earliest");
        //如果true,consumer定期地往zookeeper写入每个分区的offset
        kafkaParams.put("enable.auto.commit", CommonConstants.ENABLE_AUTO_COMMIT);
        //编码
        kafkaParams.put("serializer.encoding", CommonConstants.SERIALIZER_ENCODING);
        //能接受的最大消息大小
        kafkaParams.put("fetch.message.max.bytes",CommonConstants.FATCH_MESSAGE_MAX_BYTES);

        return kafkaParams;
    }

}
