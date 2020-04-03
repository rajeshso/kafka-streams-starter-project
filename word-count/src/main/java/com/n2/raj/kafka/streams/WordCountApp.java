package com.n2.raj.kafka.streams;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountApp {
  private static final Logger LOGGER = LoggerFactory.getLogger(WordCountApp.class);
  public static void main(String[] args) {
    final WordCountApp wordCountApp = new WordCountApp();
    final Properties config = wordCountApp.createProperties();
    final Topology topology = wordCountApp.createTopology();

    KafkaStreams kafkaStreams = new KafkaStreams(topology, config);
    // Shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    kafkaStreams.start();
    LOGGER.info(kafkaStreams.toString());
    // Update:
    // print the topology every 10 seconds for learning purposes
    while(true){
      kafkaStreams.localThreadsMetadata().forEach(data -> System.out.println(data));
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        break;
      }
    }
  }

  public Topology createTopology(){
    StreamsBuilder builder = new StreamsBuilder();
    // 1 - stream from Kafka

    KStream<String, String> textLines = builder.stream("word-count-input");
    KTable<String, Long> wordCounts = textLines
        // 2 - map values to lowercase
        .mapValues(textLine -> textLine.toLowerCase())
        // can be alternatively written as:
        // .mapValues(String::toLowerCase)
        // 3 - flatmap values split by space
        .flatMapValues(textLine -> Arrays.asList(textLine.split("\\W+")))
        // 4 - select key to apply a key (we discard the old key)
        .selectKey((key, word) -> word)
        // 5 - group by key before aggregation
        .groupByKey()
        // 6 - count occurences
        .count(Materialized.as("Counts"));

    // 7 - to in order to write the results back to kafka
    wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

    return builder.build();
  }

  private Properties createProperties() {
    Properties config = new Properties(6);
    config.put(StreamsConfig.APPLICATION_ID_CONFIG,"streams-starter-app");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
    return config;
  }
}
