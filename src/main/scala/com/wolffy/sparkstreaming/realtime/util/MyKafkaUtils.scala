package com.wolffy.sparkstreaming.realtime.util

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable

/**
 * Kafka工具类，用于生产和消费
 */
object MyKafkaUtils {

    //kafka消费配置
    private val consumerConfig: mutable.Map[String, String] = mutable.Map(

        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertiesUtils("kafka.bootstrap.servers"),
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
        ConsumerConfig.GROUP_ID_CONFIG -> "gmall",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true"
    )


    /**
     * 默认offsets位置消费
     */
    def getKafkaDStream(topic: String, ssc: StreamingContext, groupId: String): InputDStream[ConsumerRecord[String, String]] = {
        consumerConfig(ConsumerConfig.GROUP_ID_CONFIG) = groupId
        val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfig)
        )
        dStream
    }

    /**
     * 指定offsets位置消费
     */
    def getKafkaDStream(topic: String, ssc: StreamingContext, offsets: Map[TopicPartition, Long], groupId: String): InputDStream[ConsumerRecord[String, String]] = {
        consumerConfig(ConsumerConfig.GROUP_ID_CONFIG) = groupId
        val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfig, offsets)
        )
        dStream
    }

    /**
     * 创建Kafka生产者对象
     */
    def createKafkaProducer(): KafkaProducer[String, String] = {
        //Kafka生产配置
        val props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtils("kafka.bootstrap.servers"))
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

        val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
        producer
    }

    private var producer: KafkaProducer[String, String] = createKafkaProducer()

    /**
     * 生产
     */
    def send(topic: String, msg: String): Unit = {
        producer.send(new ProducerRecord[String, String](topic, msg))
    }

    /**
     * 生产 指定key
     */
    def send(topic: String, key: String, msg: String): Unit = {
        producer.send(new ProducerRecord[String, String](topic, key, msg))
    }

    /**
     * 刷写缓冲区
     */
    def flush(): Unit = {
        if (producer != null) producer.flush()
    }

    /**
     * 关闭生产者对象
     */
    def close(): Unit = {
        if (producer != null) producer.close()
    }

}
