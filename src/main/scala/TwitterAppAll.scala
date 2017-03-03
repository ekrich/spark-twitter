package org.ekrich

import java.time.Instant

import org.apache.spark
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter._ 
import org.apache.spark.util.CollectionAccumulator

import twitter4j.Status
import twitter4j.URLEntity
import twitter4j.MediaEntity
import twitter4j.TweetEntity



object TwitterAppAll {
  type Tweet = Status
  type Hashtag = Tuple2[String, Int]
  case class Hashtags(tag: String, count: Int)
  def toBit(b: Boolean): Int = if (b) 1 else 0
  def withUrl[T <: URLEntity](urls: Array[T]): Boolean = urls.length > 0
  
  
//  def updateHashtags(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
//    val newCount = ...  // add the new values with the previous running count to get the new count
//    Some(newCount)
//  }
  
  def main(args: Array[String]) {
    // https://stream.twitter.com/1.1/statuses/sample.json
    val conf = new SparkConf()
        .setAppName("Twitter Stream")
        .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    // If internal HOCON config was needed instead of a twitter4j.properties file
    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
//    val config = ConfigFactory.load()   
//    System.setProperty("twitter4j.oauth.consumerKey", config.getString("twitter.apiKey"))
//    System.setProperty("twitter4j.oauth.consumerSecret", config.getString("twitter.apiSecret"))
//    System.setProperty("twitter4j.oauth.accessToken", config.getString("twitter.token"))
//    System.setProperty("twitter4j.oauth.accessTokenSecret", config.getString("twitter.tokenSecret"))

    // stream read interval
    val seconds = 10
    val delay = Seconds(seconds)
    
    // stuff to keep track of
    val numTweets = sc.longAccumulator("NumberOfTweets")
    val numTweetsWithUrl = sc.longAccumulator("NumberOfTweetsWithUrls")
    val numTweetsWithMediaUrl = sc.longAccumulator("NumberOfTweetsWithMediaUrls")
    val topHashtags = new CollectionAccumulator[Hashtag]()
    sc.register(topHashtags, "TopHashtags")
    
    def toBit(b: Boolean): Int = if (b) 1 else 0
    def withUrl[T <: URLEntity](urls: Array[T]): Boolean = urls.length > 0
    def reduceByKey[K,V](collection: Traversable[Tuple2[K,V]])(implicit num: Numeric[V]): Map[K,V] = {
      import num._
      collection
      .groupBy(_._1)
      .map { case (group, traversable) => traversable.reduce{(a,b) => (a._1, a._2 + b._2)} }
    }
    
    val startTime = sc.startTime
    val start = Instant.ofEpochMilli(startTime)
    
    // could use the above to set env props if needed
    val tweets: DStream[Tweet] = TwitterUtils.createStream(ssc, None)
    
    val windowedTweets = tweets.window(delay)
    val urlEntities = windowedTweets.map { tweet => tweet.getURLEntities }
    val numTweetsWithUrls = urlEntities.filter { _.length > 0 }.count()
    val mediaEntities = windowedTweets.map { tweet => tweet.getMediaEntities }
    val hashtagEntities = windowedTweets.flatMap { 
        tweet => tweet.getHashtagEntities 
      }.map {
        hte => (hte.getText, 1)
      }.reduceByKey {
        (a, b) => a + b
      }.map {
        case (tag, count) => Hashtags(tag, count)
      }
      
    val sqlContext = SparkSession.builder().getOrCreate()
    import sqlContext.implicits._
    hashtagEntities.foreachRDD { rdd => 
      val df = rdd.toDF()
      df.show()
    }
 
      
    windowedTweets.foreachRDD { rdd =>
      rdd.cache()
      val urlEntities = rdd.map { tweet => tweet.getURLEntities }
      val withUrls = urlEntities.filter { _.length > 0 }.count()
      val mediaEntities = rdd.map { tweet => tweet.getMediaEntities }
      val withMediaUrls = mediaEntities.filter { _.length > 0 }.count()
     
      val hashtagEntities = rdd.flatMap { 
          tweet => tweet.getHashtagEntities 
        }.map {
          hte => (hte.getText, 1)
        }.reduceByKey {
          (a, b) => a + b
        }
//        }.map {
//          case (tag, count) => Hashtags(count, tag)
//        }
      // play with sql
      val hteDf = hashtagEntities.toDF("tag", "count")
      hteDf.show(200)
      hteDf.createOrReplaceTempView("hashtags")
        
      // add stuff up
      numTweetsWithUrl.add(withUrls)
      numTweetsWithMediaUrl.add(withMediaUrls)
      hashtagEntities.foreach { pair =>
        topHashtags.add(pair)
        
      }
      
      import scala.collection.JavaConverters._
      val sBuffer = topHashtags.value.asScala
      val map = reduceByKey(sBuffer)
      println("Top Hashtags: " + map)
 
      val rddSize = rdd.count()
      println("RDD size: " + rddSize)
      numTweets.add(rddSize)
      
      val now = System.currentTimeMillis()
      println("Total Tweets rdd: " + numTweets.sum + " Elapsed: " + numTweets.count * seconds + " sec :" + (now - startTime) + "\n"
          + "Url: " + numTweetsWithUrl.sum + " Media: " + numTweetsWithMediaUrl.sum
          + "\n" + topHashtags)

      
      rdd.foreach { tweet => 
          val urls = tweet.getURLEntities
          numTweetsWithUrl.add(toBit(withUrl(urls)))
          val mediaUrls = tweet.getMediaEntities
          numTweetsWithMediaUrl.add(toBit(withUrl(mediaUrls)))
          
//        tweet.getURLEntities.foreach { url => 
//          println(url.getExpandedURL) 
//        } 
      }
      println("Total Tweets: " + numTweets.sum + " Elapsed: " + numTweets.count * seconds + " sec :" + (now - startTime) + "\n"
          + "Url: " + numTweetsWithUrl.sum + " Media: " + numTweetsWithMediaUrl.sum)
     
    }
    println("Num: " + numTweetsWithUrls.map { x => x })
    
    // Print popular hashtags
    hashtagEntities.foreachRDD {
      rdd => {
        val topList = rdd.take(10)
        println("\nPopular topics in last " + seconds + " seconds (%s total):".format(rdd.count()))
        topList.foreach{case Hashtags(count, tag) => println("%s (%s tweets)".format(tag, count))}
      }
    }
    
    ssc.start()
    ssc.awaitTermination()
    
  }
}