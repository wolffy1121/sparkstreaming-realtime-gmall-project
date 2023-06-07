package com.wolffy.sparkstreaming.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.wolffy.sparkstreaming.realtime.util.{MyKafkaUtils, MyOffsetUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import redis.clients.jedis.Jedis


/**
 * 业务数据分流
 * 1. 读取偏移量
 * 2. 接收kakfa数据
 * 3. 提取偏移量结束点
 * 4. 转换结构
 * 5. 分流处理
 *    5.1 事实数据分流-> kafka
 *    5.2 维度数据分流-> redis
 * 6. 提交偏移量
 */
object OdsBaseDbAPP {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("base_db_app").setMaster("local[4]")
        val ssc = new StreamingContext(sparkConf,Seconds(5))

        val topic = "ODS_BASE_DB_M"
        val groupId = "base_db_group"


        //1.读取偏移量
        val offsets: Map[TopicPartition, Long] = MyOffsetUtils.getOffset(topic,groupId)

        //2. 接收kafka数据
        var kafkaDStream : DStream[ConsumerRecord[String,String ]]  = null
        if(offsets != null && offsets.nonEmpty){
            kafkaDStream = MyKafkaUtils.getKafkaDStream(topic,ssc,offsets,groupId)
        }else{
            kafkaDStream = MyKafkaUtils.getKafkaDStream(topic,ssc,groupId)
        }



        //3. 提取偏移量结束点
        var  offsetRanges: Array[OffsetRange] = null
        kafkaDStream  = kafkaDStream.transform(
            rdd => {
                offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        )

        //4. 转换结构
        val jsonObjDstream: DStream[JSONObject] = kafkaDStream.map(
            record => {
                val jsonString: String = record.value()
                val jSONObject: JSONObject = JSON.parseObject(jsonString)
                jSONObject
            }
        )

        //5. 分流

        jsonObjDstream.print(100)
        // 5.1 事实数据
        // 5.2 维度数据

//        jsonObjDstream.foreachRDD(
//            rdd => {
//                val jedis: Jedis = MyRedisUtils.getJedisClient
//                val dimTableKey : String = "DIM:TABLES"
//                val factTableKey : String = "FACT:TABLES"
//                //从redis中读取表清单
//                val dimTables: util.Set[String] = jedis.smembers(dimTableKey)
//                val factTables: util.Set[String] = jedis.smembers(factTableKey)
//                println("检查维度表: " + dimTables)
//                println("检查事实表: " + factTables)
//                //做成广播变量
//                val dimTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dimTables)
//                val factTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(factTables)
//
//                jedis.close()
//
//
//                //只要有一个批次执行一次，且在executor中执行的代码， 就需要用foreachPartittion
//                rdd.foreachPartition(
//                    jsonObjIter => {
//                        //获取redis连接
//                        val jedis: Jedis = MyRedisUtils.getJedisClient
//                        for (jsonObj <- jsonObjIter) {
//                            //提取表名
//                            val tableName: String = jsonObj.getString("table")
//                            //提取操作类型
//                            val optType: String = jsonObj.getString("type")
//                            val opt: String = optType match {
//                                case "insert" => "I"
//                                case "update" => "U"
//                                case "delete" => "D"
//                                case _ => null  // DDL操作，例如: CREATE ALTER TRUNCATE ....
//                            }
//                            if(opt != null ){
//
//                                //提取修改后的数据
//                                val dataJsonArray: JSONObject = jsonObj.getJSONObject("data")
//                                //事实表数据
//                                if(factTablesBC.value.contains(tableName)){
//                                    // 拆分到指定的主题
//                                    // topic => DWD_[TABLE_NAME]_[I/U/D]
//                                    val topicName = s"DWD_${tableName.toUpperCase()}_$opt"
//
//                                    val key: String = dataJsonArray.getString("id")
//                                    //发送kafka
//                                    MyKafkaUtils.send(topicName,key, dataJsonArray.toJSONString)
//
//                                }
//
//                                //维度表处理
//                                if(dimTablesBC.value.contains(tableName)){
//                                    //val jedis: Jedis = MyRedisUtils.getJedisClient
//                                    // 存储类型的选择： String 、 set 、 hash ?
//                                    // key : DIM:[table_name]:[主键]
//                                    // value : 整条数据的json串
//                                    val id: String = dataJsonArray.getString("id")
//                                    val redisKey : String = s"DIM:${tableName.toUpperCase()}:$id"
//                                    val redisValue : String = dataJsonArray.toJSONString
//                                    jedis.set(redisKey,redisValue)
//
//                                    //jedis.close()
//                                }
//                            }
//                        }
//                        jedis.close()
//                        MyKafkaUtils.flush()
//                    }
//                )
//                //6.提交偏移量
//                MyOffsetUtils.saveOffset(topic,groupId,offsetRanges)
//            }
//        )



        ssc.start()
        ssc.awaitTermination()
    }

}
