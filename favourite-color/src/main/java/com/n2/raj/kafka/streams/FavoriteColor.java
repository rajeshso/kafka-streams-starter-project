package com.n2.raj.kafka.streams;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//bin/kafka-topics.sh --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --create --topic color-count-input1
//bin/kafka-topics.sh --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --create --topic color-count-input1-compacted --config cleanup.policy=compact --config min.cleanable.dirty.ratio=0.005 --config segment.ms=10000
//bin/kafka-topics.sh --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --create --topic color-count-output1 --config cleanup.policy=compact
//bin/kafka-console-producer.sh --broker-list localhost:9092 --topic color-count-input1 --property parse.key=false
//bin/kafka-topics.sh -zookeeper localhost:2181 --list
// bin/kafka-topics.sh -zookeeper localhost:2181 --delete --topic color-count-output1
/**
 bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic color-count-output \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 */
public class FavoriteColor {

  private static final Logger LOGGER = LoggerFactory.getLogger(FavoriteColor.class);
  private static final  String INPUT_TOPIC = "color-count-input1";
  private static final  String COMPACTED_TOPIC = "color-count-input1-compacted";
  private static final String OUTPUT_TOPIC = "color-count-output1";
  public static final String COLOR_STREAMS_STARTER_APP = "color-streams-starter-app";

  public static void main(String[] args) {


    final FavoriteColor favoriteColor = new FavoriteColor();
    final Properties config = favoriteColor.createProperties();
    final Topology topology = favoriteColor.createTopology();

    KafkaStreams kafkaStreams = new KafkaStreams(topology, config);
    // Shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    //do this only in dev
    kafkaStreams.cleanUp();
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
    StreamsBuilder builder1 = new StreamsBuilder();
    // 1 - stream from Kafka

    KStream<String, String> textLineStream = builder1.stream(INPUT_TOPIC);
    final KStream<String, String> intermediateStream = textLineStream
        .mapValues(textLine -> textLine.toLowerCase())
        //Filter bad values
        .filter((k, textLine) -> textLine.contains(","))
        .filter((k, textLine) -> textLine.split(",").length == 2)
        //key value
        //.flatMapValues((textLine) -> Arrays.asList(textLine.split(",")))
        //Select Key that will be the user id
        .selectKey((k, v) -> v.split(",")[0])
        //MapValues to extract the color(as lowercase)
        .mapValues((k, v) -> v.split(",")[1])
        .filter((user, colour) -> Arrays.asList("green", "blue", "red").contains(colour));

    //Filter to remove bad colours
    //Write to Kafka as intermediary topic
    intermediateStream.to(COMPACTED_TOPIC);

    //Read from Kafka as a KTable
    Serde<String> stringSerde = Serdes.String();
    Serde<Long> longSerde = Serdes.Long();
    final KTable<String, String> compactedTable = builder1.table(COMPACTED_TOPIC);
    final KTable<String, Long> colorCount = compactedTable
        .groupBy((name, color) -> new KeyValue<>(color, color))
        .count(Materialized.with(stringSerde, longSerde).as("countsByColors"));

    //GroupBy colours
    //Count to count Colours occurrences (KTable)
    //Write to Kafka as final topic
    colorCount.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

    return builder1.build();
  }

  private Properties createProperties() {
    Properties config = new Properties(5);
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, COLOR_STREAMS_STARTER_APP);
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
    config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
    return config;
  }
}
