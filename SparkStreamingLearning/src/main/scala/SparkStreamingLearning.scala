package main.scala

import org.apache.spark.streaming._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkStreamingLearning extends App {
  val conf = new SparkConf().setAppName("SparkStreamingLearning").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(10))
  val streamRDD =   ssc.socketTextStream("127.0.0.1",2222)
  val wordcounts = streamRDD.flatMap(line =>   line.split(" ")).map(word => (word,1)).reduceByKey(_+_)
  wordcounts.print()
  ssc.start()
  ssc.awaitTermination()  
}