package net.mkrcah

import java.util.Properties

import com.twitter.bijection.avro.SpecificAvroCodecs.{toJson, toBinary}
import com.typesafe.config.ConfigFactory
import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import net.mkrcah.TwitterStream.OnTweetPosted
import net.mkrcah.avro.Tweet
import twitter4j.{Status, FilterQuery}

object KafkaProducerApp {

  private val conf = ConfigFactory.load()

  val KafkaTopic = "tweets"

  val kafkaProducer = {
    val props = new Properties()
    props.put("metadata.broker.list", conf.getString("kafka.brokers"))
    props.put("request.required.acks", "1")
    val config = new ProducerConfig(props)
    new Producer[String, Array[Byte]](config)
  }

  def main (args: Array[String]) {
    val twitterStream = TwitterStream.getStream
      TwitterStream.keyword_=("Donald Trump")
      var keyword = TwitterStream._keyword
    twitterStream.addListener(new OnTweetPosted(s => sendToKafka(toTweet(s))))
    twitterStream.filter(new FilterQuery().track(keyword.split(",")))
  }

  private def toTweet(s: Status): Tweet = {
    new Tweet(s.getUser.getName, s.getText)
  }

  private def sendToKafka(t:Tweet) {
    println(toJson(t.getSchema).apply(t))
    val tweetEnc = toBinary[Tweet].apply(t)
    val msg = new KeyedMessage[String, Array[Byte]](KafkaTopic, tweetEnc)
    kafkaProducer.send(msg)
  }

}



