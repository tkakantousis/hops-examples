package io.hops.examples.spark;

import io.hops.util.Hops;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.logging.Logger;

public class HudiFeatureStore {
  static Logger logger = Logger.getLogger(HudiFeatureStore.class.getName());
  
  public static void main(String[] args) {
    
    SparkConf sparkConf = new SparkConf();
    SparkSession spark = SparkSession
      .builder()
      .appName(Hops.getJobName())
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate();
    
    logger.info("Hello Feature Store! version:" + spark.version());
    
    Dataset ds = spark.read().json("hdfs:///Projects/Hudi/DataSource/batch_1.json");
    Hops.createFeaturegroup("likeanything").setDataframe(ds);
    //Stop spark session
    spark.stop();
    
  }
}
