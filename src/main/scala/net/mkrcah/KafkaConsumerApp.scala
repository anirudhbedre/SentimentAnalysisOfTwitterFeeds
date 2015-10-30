package net.mkrcah

import com.typesafe.config.ConfigFactory
import kafka.serializer.{DefaultDecoder, StringDecoder}
import com.twitter.bijection.avro.SpecificAvroCodecs
import net.mkrcah.avro.Tweet
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import edu.stanford.nlp.io
import edu.stanford.nlp.ling
import edu.stanford.nlp.trees
import edu.stanford.nlp.util
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.pipeline
import edu.stanford.nlp.time
import edu.stanford.nlp.util.CoreMap
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation
import java.util.{List => JList}
import scala.collection.JavaConversions._
import java.lang.Object
import java.util.Properties
import java.util.List
import java.util.Calendar
import org.apache.spark.sql.SQLContext
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.annotation.Experimental
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.{DataWriteMethod, OutputMetrics}
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.collection.CompactBuffer
import org.apache.spark.util.random.StratifiedSamplingUtils
import org.apache.spark.Logging
import org.apache.spark.rdd.PairRDDFunctions
import scala.Serializable
import org.bson.BasicBSONObject
import org.apache.hadoop.conf.Configuration
import org.bson.BSONObject
import com.mongodb.hadoop.{
  MongoInputFormat, MongoOutputFormat,
  BSONFileInputFormat, BSONFileOutputFormat}


object KafkaConsumerApp extends App{

  private val conf = ConfigFactory.load()
  val sparkConf = new SparkConf().setAppName("kafka-twitter-spark-example").setMaster("local[*]")
  val ssc = new SparkContext(sparkConf)
  val sc = new StreamingContext(ssc, Seconds(10))
  val sqlContext = new SQLContext(ssc)
  val config = new Configuration()
  config.set("mongo.input.uri", "mongodb://localhost:27017/twitter.twitter")
  config.set("mongo.output.uri", "mongodb://localhost:27017/twitter.twitter")    
     
  val encTweets = {
    val topics = Map(KafkaProducerApp.KafkaTopic -> 1)
    val kafkaParams = Map(
    "zookeeper.connect" -> conf.getString("kafka.zookeeper.quorum"),
    "group.id" -> "1")
    KafkaUtils.createStream[String, Array[Byte], StringDecoder, DefaultDecoder](
    sc, kafkaParams, topics, StorageLevel.MEMORY_ONLY)
    }
    val tweets = encTweets.flatMap(x => SpecificAvroCodecs.toBinary[Tweet].invert(x._2).toOption)
    def sentiment(text:String) : String = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")    
    val pipeline = new  StanfordCoreNLP(props)
    val annotation = pipeline.process(text)
    val sentences : java.util.List[CoreMap] = annotation.get(classOf[SentencesAnnotation])
    var senti = "" 
    for (sentence <- sentences){
    val sentiment = sentence.get(classOf[SentimentCoreAnnotations.SentimentClass])
    senti = sentiment    
    }
    senti 
    }

    val sentiments = tweets.map(twt => sentiment(twt.getText)).map((_,1)).reduceByKey(_ + _)
    val sentiments2 = sentiments.reduceByKey(_ + _)
    val countsSorted = sentiments2.transform(_.sortBy(_._2, ascending = false))
    val countSorted2 = countsSorted.reduceByKey(_ + _)
    

    val saveRDD = countSorted2.map((tuple) => {
    var bson = new BasicBSONObject()
    var twt_time = Calendar.getInstance().getTime()
    
    bson.put("timestamp", twt_time.toString)
    bson.put("sentiment", tuple._1)
    bson.put("count", tuple._2.toString)
    bson.put("flag", "0")
      (null, bson)
    })
    
    saveRDD.foreachRDD(rdd => {
    val pair_rdd = new PairRDDFunctions[Null, org.bson.BasicBSONObject](rdd)
     pair_rdd.saveAsNewAPIHadoopFile("file:///bogus", classOf[Any], classOf[Any],           classOf[com.mongodb.hadoop.MongoOutputFormat[Any, Any]], config)                          
    })
    
    countSorted2.print()
    
  sc.start()
  sc.awaitTermination()
}
