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
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountApp {
  private static final Logger LOGGER = LoggerFactory.getLogger(WordCountApp.class);
  public static void main(String[] args) {
    Properties properties = createProperties();

    final Topology topology = createTopology();

    KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
    kafkaStreams.start();
    LOGGER.info(kafkaStreams.toString());

    // Shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
  }

  private static Topology createTopology() {
    StreamsBuilder builder = new StreamsBuilder();
    //1. Stream from Kafka
    final KStream<String, String> textLines = builder.stream("word-count-input");
    //2. Map Values to lowercase
    final KTable<String, Long> wordCounts = textLines.mapValues(
        (ValueMapper<String, String>) String::toLowerCase)
                  //3. flatmap values split by space
                  .flatMapValues(lowerCasedTextLine -> Arrays.asList(lowerCasedTextLine.split("")))
                  //4. select key to apply key (we discard the old key)
                  .selectKey((ignoredKey, word) -> word)
                  //5. Group by key before aggregation
                  .groupByKey()
                  //6. Count occurences
                  .count(Named.as("Counts"));
    //7. To in order to write the value back to Kafka
    wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

    return builder.build();
  }

  private static Properties createProperties() {
    Properties config = new Properties(6);
    config.put(StreamsConfig.APPLICATION_ID_CONFIG,"streams-starter-app");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
    return config;
  }
}
