package io.hops.examples.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.FileInputStream;
import java.util.Properties;

public class FlinkKafkaConsumerExample {
  
  public static void main(String[] args) throws Exception {
    final ParameterTool params = ParameterTool.fromArgs(args);
    
    // set up the execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  
    // make parameters available in the web interface
    env.getConfig().setGlobalJobParameters(params);
  
    env.setParallelism(params.getInt("parallelism", 1));
    
    //--------------------------------------------------------------------------------------------------------------
    // configure Kafka consumer
    // Data
    Properties dataKafkaProps = new Properties();
    
    //Read password from local file
    String materialPassword;
    try (FileInputStream fis = new FileInputStream("material_passwd")) {
      StringBuilder sb = new StringBuilder();
      int content;
      while ((content = fis.read()) != -1) {
        sb.append((char) content);
      }
      materialPassword = sb.toString();
    }
    
    dataKafkaProps.setProperty("bootstrap.servers", "10.0.2.15:9091");
    dataKafkaProps.setProperty("security.protocol", "SSL");
    dataKafkaProps.setProperty("ssl.truststore.location", "t_certificate");
    dataKafkaProps.setProperty("ssl.truststore.password", materialPassword);
    dataKafkaProps.setProperty("ssl.keystore.location", "k_certificate");
    dataKafkaProps.setProperty("ssl.keystore.password", materialPassword);
    dataKafkaProps.setProperty("ssl.key.password", materialPassword);
    dataKafkaProps.setProperty("ssl.endpoint.identification.algorithm", "");
    dataKafkaProps.setProperty("group.id", "something");
  
    DataStream<String> stream = env
      .addSource(new FlinkKafkaConsumer<>("flinktopic", new SimpleStringSchema(), dataKafkaProps))
      .map(s -> s.toLowerCase());

    env.execute();
    
  }
  
}