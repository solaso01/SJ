package com.sjzy.common;


import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Created by wpp on 2020/7/21.
 *
 * 相关的静态变量
 */
public class CommonConstants {

    public static final String APP_NAME = "sjzy_es01";

    public static final String KAFKA_TOPIC_NAME = "oggkafka";

    public static final String KAFKA_BOOTSTRAP_SERVERS = "192.1.6.73:9092,192.1.6.74:9092,192.1.6.75:9092";

    public static final String KUDU_MASTER = "192.1.6.73:7051";

    public static final String KAFKA_GROUP_ID = "wpp03";

    public static final Boolean ENABLE_AUTO_COMMIT = false;

    public static final String SERIALIZER_ENCODING = "utf8";

    public static final String FATCH_MESSAGE_MAX_BYTES = "10485760";

    public static final Object VALUE_DESERIALIZER = StringDeserializer.class;

    public static final Object KEY_DESERIALIZER = StringDeserializer.class;

    public static final Integer SECOND = 1;

    public static final Integer MINUTE = 60;

    public static final String SPARK_DEFAULT_PARALLELISM = "10";

    public static final String SPARK_SQL_SHUFFLE_PARTITIONS = "10";

    public static final String SPARK_SERIALIZER = "org.apache.spark.serializer.KryoSerializer";

    public static final String SPARK_SQL_CROSSJOIN_ENABLED = "true";

    public static final String MYSQL_URL = "jdbc:mysql://192.1.6.73:3306/kafka";

    public static final String MYSQL_USER = "root";

    public static final String MYSQL_PASSWORD = "sj123456@#1";

    public static final String MYSQL_MAP_TABLE_NAME = "kafka.es_tb_mapping";

    public static final String ES_HOST_NAME = "192.1.6.74";

    public static final Integer ES_PORT = 9200;

    public static final Integer ES_SINGLE_FLUSH_NUM = 1000;

    public static final String GET_KAFKA_TOPIC_OFFSET_SQL = "select topic,partitiona,fromoffset from kafka.offsetloges where type='ogg' ";

}

