/*
 * 
 * Spark application which perform analysis on tweets data and  get the count of each hashtags
 * Source file taken from the problem statement
 */


package scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
object TwitterDataAnalysis {

  def main(args: Array[String]): Unit = {

    //creating an instance of SparkConf to provide the spark configurations.This will make spark to run in local mode
    val conf = new SparkConf().setAppName("Twitter Analaysis").setMaster("local")

    //Providing configuration parameter to SparkContext with an  instance of SparkConf
    val sc = new SparkContext(conf)

    //create hiveContext to create dataframe
    val sqlContext = new HiveContext(sc)

    //Create the dataframe from json file and register the dataframe to a temporary table tweets which can used to query through sql query
    val tweets = sqlContext.read.json("/home/acadgild/scala_eclipse/TweetSample").registerTempTable("tweets")

    //Create the dataframe which stores the id and hashtags used from tweets table and register  the dataframe to a temporary table hashtags which can used to query through sql query
    val hashtags = sqlContext.sql("select id as id,entities.hashtags.text as words from tweets").registerTempTable("hashtags")

    //Create the dataframe which stores the id and flatten of hashtags used from hashtag table and register  the dataframe to a temporary table hashtag_word which can used to query through sql query
    val hashtag_word = sqlContext.sql("select id as id,hashtag from hashtags LATERAL VIEW explode(words) w as hashtag").registerTempTable("hashtag_word")

    //create the datafram which stores the count for the hashtag used from hashtag_word table and display using action show
    val popular_hashtags = sqlContext.sql("select hashtag, count(hashtag) as cnt from hashtag_word group by hashtag order by cnt desc").show

  }
}