/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.hops.examples.spark.kafka;

import com.google.common.base.Strings;
import com.twitter.bijection.Injection;
import io.hops.util.HopsProducer;
import io.hops.util.Hops;
import io.hops.util.exceptions.JWTNotFoundException;
import io.hops.util.exceptions.SchemaNotFoundException;
import io.hops.util.spark.SparkProducer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

/**
 *
 * <p>
 */
public class StructuredStreamingKafka {

  private static final Map<String, Injection<GenericRecord, byte[]>> RECORD_INJECTIONS = Hops.getRecordInjections();
  private static final Logger LOG = Logger.getLogger(StructuredStreamingKafka.class.getName());

  public static void main(String[] args) throws StreamingQueryException, InterruptedException {
    final String type = args[0];
    //Producer
    if (!Strings.isNullOrEmpty(type) && type.equalsIgnoreCase("producer")) {
      Set<String> topicsSet = new HashSet<>(Hops.getTopics());
      SparkConf sparkConf = new SparkConf().setAppName(Hops.getJobName());
      JavaSparkContext jsc = new JavaSparkContext(sparkConf);
      final List<HopsProducer> sparkProducers = new ArrayList<>();
      final DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS");
      final List<String> messages = new ArrayList<>();
      final List<String> priorities = new ArrayList<>();
      final List<String> loggers = new ArrayList<>();

      /*
       * ********************************* Setup dummy test data ***********************************
       */
      messages.add("Container container_e01_1494850115055_0016_01_000002 succeeded");
      messages.add("Container container_e01_1494850115251_0015_01_000002 succeeded");
      messages.add("rollingMonitorInterval is set as -1. The log rolling mornitoring interval is disabled. "
          + "The logs will be aggregated after this application is finished.");
      messages.add("rollingMonitorInterval is set as -1. The log rolling mornitoring interval is disabled. "
          + "The logs will be aggregated after this application is finished.");
      messages.add("Sending out 2 container statuses: "
          + "[ContainerStatus: [ContainerId: container_e01_1494850115055_0016_01_000001, State: RUNNING, "
          + "Diagnostics: , ExitStatus: -1000, ], "
          + "ContainerStatus: [ContainerId: container_e01_1494850115055_0016_01_000002, "
          + "State: RUNNING, Diagnostics: , ExitStatus: -1000, ]]");
      messages.add("Node's health-status : true");
      messages.add("Cannot create writer for app application_1494433225517_0008. Skip log upload this time.");
      priorities.add("INFO");
      priorities.add("INFO");
      priorities.add("WARN");
      priorities.add("DEBUG");
      priorities.add("DEBUG");
      priorities.add("DEBUG");
      priorities.add("ERROR");
      loggers.add("org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl");
      loggers.add("org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl");
      loggers.add("org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.AppLogAggregatorImpl");
      loggers.add("org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdaterImpl");
      loggers.add("org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdaterImpl");
      loggers.add("org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdaterImpl");
      loggers.add("org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.AppLogAggregatorImpl");
      //End setup dummy data

      //Get a broker for the producer
      LOG.log(Level.INFO, "Producing to:{0}", Hops.getBrokerEndpointsList().get(0));
      Properties props = new Properties();
      props.put("bootstrap.servers", Hops.getBrokerEndpointsList().get(0));
      for (final String topic : topicsSet) {
        new Thread() {
          @Override
          public void run() {
            try {
              SparkProducer sparkProducer = Hops.getSparkProducer(topic, props);
              sparkProducers.add(sparkProducer);
              Map<String, String> message = new HashMap<>();
              int i = 0;
              //Produce Kafka messages to topic
              while (true) {
                message.put("message", messages.get(i % messages.size()));
                message.put("priority", priorities.get(i % priorities.size()));
                message.put("logger", loggers.get(i % loggers.size()));
                Date date = new Date();
                message.put("timestamp", sdf.format(date));
                sparkProducer.produce(message);
                Thread.sleep(100);
                i++;
              }
            } catch (SchemaNotFoundException | JWTNotFoundException | InterruptedException ex) {
              LOG.log(Level.SEVERE, ex.getMessage(), ex);
            }
          }
        }.start();
      }//Keep application running
      Hops.shutdownGracefully(jsc);
      for (HopsProducer hopsProducer : sparkProducers) {
        hopsProducer.close();
      }
      //Consumer
    } else {
      // Create DataSet representing the stream of input lines from kafka
      DataStreamReader dsr = Hops.getSparkConsumer().getKafkaDataStreamReader();
      Dataset<Row> lines = dsr.load();

      // Generate running word count
      Dataset<LogEntry> logEntries = lines
          .map(new MapFunction<Row, LogEntry>() {
            @Override
            public LogEntry call(Row record) throws Exception {
              GenericRecord genericRecord = RECORD_INJECTIONS.entrySet().iterator().next().getValue().invert(record.
                  getAs("value")).get();
              return new LogEntry(genericRecord.get("timestamp").toString(),
                  genericRecord.get("priority").toString(),
                  genericRecord.get("logger").toString(),
                  genericRecord.get("message").toString());
            }
          }, Encoders.bean(LogEntry.class));

      Dataset<String> logEntriesRaw = lines
          .map(new MapFunction<Row, String>() {
            @Override
            public String call(Row record) throws Exception {
              GenericRecord genericRecord = RECORD_INJECTIONS.entrySet().iterator().next().getValue().invert(record.
                  getAs("value")).get();

              return genericRecord.toString();
            }
          }, Encoders.STRING());

      // Start running the query that prints the running counts to the console
      StreamingQuery queryFile = logEntries.writeStream()
          .format("parquet")
          .option("path", "/Projects/" + Hops.getProjectName() + "/Resources/data-parquet-" + Hops.getAppId())
          .option("checkpointLocation", "/Projects/" + Hops.getProjectName() + "/Resources/checkpoint-parquet-"
              + Hops.getAppId())
          .trigger(Trigger.ProcessingTime(10000))
          .start();

      logEntriesRaw.writeStream()
          .format("text")
          .option("path", "/Projects/" + Hops.getProjectName() + "/Resources/data-text-" + Hops.getAppId())
          .option("checkpointLocation", "/Projects/" + Hops.getProjectName() + "/Resources/checkpoint-text-"
              + Hops.getAppId())
          .trigger(Trigger.ProcessingTime(10000))
          .start();

      Hops.shutdownGracefully(queryFile);
    }
  }
}
