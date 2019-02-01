package io.hops.examples.spark.kafka;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class ConsumerHiveJava {
  
  public static void main(String[] args) {
    
    SparkConf sparkConf = new SparkConf().setAppName("ConsumerHive");
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
    
  }
  
}
