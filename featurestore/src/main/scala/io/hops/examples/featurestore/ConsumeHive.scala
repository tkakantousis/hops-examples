package io.hops.examples.featurestore

import io.hops.util.Hops
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}
import org.apache.spark.sql.types._


object ConsumeHive {


  def main(args: Array[String]): Unit = {

    var sparkConf: SparkConf = null
    sparkConf = new SparkConf().setAppName("ConsumerHive")

    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val mySchema = StructType(Array())


    val dfTo = spark.readStream.format("kafka").
      option("kafka.bootstrap.servers", args(3)).
      option("kafka.security.protocol", "SSL").
      option("kafka.ssl.truststore.location", Hops.getTrustStore).
      option("kafka.ssl.truststore.password", Hops.getKeystorePwd).
      option("kafka.ssl.keystore.location", Hops.getKeyStore).
      option("kafka.ssl.keystore.password", Hops.getKeystorePwd).
      option("kafka.ssl.key.password", Hops.getKeystorePwd).
      //option("spark.streaming.backpressure.enabled", true).
      option("auto.offset.reset", "earliest").
      option("startingOffsets", "earliest").
      option("subscribe", args(0)).load()

    val query = dfTo.coalesce(args(3).toInt).select($"value" cast "string" as "json").select(from_json($"json",
      mySchema) as
      "data").select("data.*").writeStream.
      format("parquet").
      option("path", "/Projects/" + Hops.getProjectName + "/" + args(1)).
      option("checkpointLocation", "/Projects/" + Hops.getProjectName + "/" + args(2)).
      trigger(Trigger.ProcessingTime("120 seconds")).
      start()

    query.awaitTermination()
    spark.close()
  }
}
