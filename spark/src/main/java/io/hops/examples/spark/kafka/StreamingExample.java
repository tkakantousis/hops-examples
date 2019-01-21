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
import io.hops.util.spark.SparkConsumer;
import io.hops.util.spark.SparkProducer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import scala.Tuple2;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount,
 * produces
 * hello world messages to Kafka using Hops Kafka Producer. Streaming code based
 * on Spark JavaDirectKafkaWordCount.
 * Usage: StreamingExample <type> <sink>
 * <type> type of kafka process (producer|consumer)
 * <sink> location in hdfs to append streaming output
 * <p>
 * Example:
 * $ bin/run-example streaming.StreamingExample
 * consumer /Projects/MyProject/Sink/Data
 * <p>
 */
public final class StreamingExample {

  private static final Logger LOG = Logger.getLogger(StreamingExample.class.getName());
  private static final Pattern SPACE = Pattern.compile(" ");
  //Get HopsWorks Kafka Utility instance
  private static final Map<String, Injection<GenericRecord, byte[]>> recordInjections
      = Hops.getRecordInjections();

  public static void main(final String[] args) throws Exception {
    if (args.length < 1) {
      LOG.log(Level.SEVERE, "Usage: StreamingExample <type> <sink> <topics> \n"
          + "  <type> type of kafka process (producer|consumer).\n"
          + "  <sink> location in hdfs to append streaming output.\n\n");
      System.exit(1);
    }

    final String type = args[0];
    // Create context with a 2 ; batch interval
    Set<String> topicsSet = new HashSet<>(Hops.getTopics());
    SparkConf sparkConf = new SparkConf().setAppName("StreamingExample");
    final List<HopsProducer> sparkProducers = new ArrayList<>();

    if (!Strings.isNullOrEmpty(type) && type.equalsIgnoreCase("producer")) {
      JavaSparkContext jsc = new JavaSparkContext(sparkConf);
      //Create a producer for each topic
      for (final String topic : topicsSet) {
        new Thread() {
          @Override
          public void run() {
            try {
              SparkProducer sparkProducer = Hops.getSparkProducer(topic);
              sparkProducers.add(sparkProducer);
              Map<String, String> message = new HashMap<>();
              int i = 0;
              //Produce Kafka messages to topic
              while (true) {
                message.put("platform", "HopsWorks");
                message.put("program", "SparkKafka-" + topic + "-" + i);
                sparkProducer.produce(message);
                Thread.sleep(1000);
                i++;
              }
            } catch (SchemaNotFoundException | JWTNotFoundException | InterruptedException ex) {
              Logger.getLogger(StreamingExample.class.getName()).
                  log(Level.SEVERE, null, ex);
            }
          }
        }.start();
      }//Keep application running
      Hops.shutdownGracefully(jsc);
    } else {
      JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
          Durations.seconds(2));
      //Use applicationId for sink folder
      final String appId = jssc.sparkContext().getConf().getAppId();

      //Get consumer groups
      List<String> consumerGroups = Hops.getConsumerGroups();
      SparkConsumer consumer = Hops.getSparkConsumer(jssc, topicsSet);
      // Create direct kafka stream with topics
      JavaInputDStream<ConsumerRecord<String, byte[]>> messages = consumer.
          createDirectStream();

      //Get the schema for which to consume messages
      final StringBuilder line = new StringBuilder();

      // Get the lines, split them into words, count the words and print
      JavaDStream<String> lines = messages.map(new Function<ConsumerRecord<String, byte[]>, String>() {
        @Override
        public String call(ConsumerRecord<String, byte[]> record) throws
            SchemaNotFoundException {
          line.setLength(0);
          //Parse schema and generate Avro record
          //For this example, we use a single schema so we get the first record
          //of the recordInjections map. Otherwise do
          //recordInjections.get("topic");
          GenericRecord genericRecord = recordInjections.entrySet().iterator().
              next().getValue().invert(record.value()).get();
          line.append(((Utf8) genericRecord.get("platform")).toString()).
              append(" ").
              append(((Utf8) genericRecord.get("program")).toString());
          return line.toString();
        }
      });

      JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
        @Override
        public Iterator<String> call(String x) {
          return Arrays.asList(SPACE.split(x)).iterator();
        }
      });

      JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
        @Override
        public Tuple2<String, Integer> call(String s) {
          return new Tuple2<>(s, 1);
        }
      }).reduceByKey(new Function2<Integer, Integer, Integer>() {
          @Override
          public Integer call(Integer i1, Integer i2) {
            return i1 + i2;
          }
        });

      wordCounts.print();

      /*
       * Based on Spark Design patterns
       * http://spark.apache.org/docs/latest/streaming-programming-guide.html#output-operations-on-dstreams
       */
      wordCounts.foreachRDD(new VoidFunction2<JavaPairRDD<String, Integer>, Time>() {
        @Override
        public void call(JavaPairRDD<String, Integer> rdd, Time time) throws
            Exception {
          //Keep the latest microbatch output in the file
          rdd.repartition(1).saveAsHadoopFile(args[1] + "-" + appId,
              String.class,
              String.class,
              TextOutputFormat.class);
        }

      });

      /*
       * Enable this to get all the streaming outputs. It creates a folder for
       * every microbatch slot.
       * ///////////////////////////////////////////////////////////////////////
       * wordCounts.saveAsHadoopFiles(args[1], "txt", String.class,
       * String.class, (Class) TextOutputFormat.class);
       * ///////////////////////////////////////////////////////////////////////
       */
      // Start the computation
      jssc.start();
      Hops.shutdownGracefully(jssc);
    }
    for (HopsProducer hopsProducer : sparkProducers) {
      hopsProducer.close();
    }
  }
}
