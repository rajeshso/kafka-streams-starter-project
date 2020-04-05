package com.n2.raj.kafka.streams;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

//bin/kafka-topics.sh --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --create --topic bank-transactions
//bin/kafka-topics.sh --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --create --topic bank-balance-exactly-once
//bin/kafka-topics.sh -zookeeper localhost:2181 --list
// bin/kafka-topics.sh -zookeeper localhost:2181 --delete --topic bank-balance-exactly-once
// bin/kafka-topics.sh -zookeeper localhost:2181 --delete --topic bank-transactions
/**
 bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
 --topic bank-balance-exactly-once \
 --from-beginning \
 --formatter kafka.tools.DefaultMessageFormatter \
 --property print.key=true \
 --property print.value=true \
 --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
 --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
 */
/**
 bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
 --topic bank-transactions \
 --from-beginning \
 --formatter kafka.tools.DefaultMessageFormatter \
 --property print.key=true \
 --property print.value=true \
 --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
 --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
 */
public class BankTransactionsProducer {

    public static final String TOPIC = "bank-transactions";

    public static void main(String[] args) {
        Properties properties = new Properties();

        // kafka bootstrap server
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // producer ackst
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // strongest producing guarantee
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        // leverage idempotent producer from Kafka 0.11 !
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // ensure we don't push duplicates

        Producer<String, String> producer = new KafkaProducer<>(properties);

        int i = 0;
        while (true) {
            System.out.println("Producing batch: " + i);
            try {
                producer.send(newRandomTransaction("john"));
                Thread.sleep(100);
                producer.send(newRandomTransaction("stephane"));
                Thread.sleep(100);
                producer.send(newRandomTransaction("alice"));
                Thread.sleep(100);
                i += 1;
            } catch (InterruptedException e) {
                break;
            }
        }
        producer.close();
    }

    public static ProducerRecord<String, String> newRandomTransaction(String name) {
        // creates an empty json {}
        ObjectNode transaction = JsonNodeFactory.instance.objectNode();

        // { "amount" : 46 } (46 is a random number between 0 and 100 excluded)
        Integer amount = ThreadLocalRandom.current().nextInt(0, 100);

        // Instant.now() is to get the current time using Java 8
        Instant now = Instant.now();

        // we write the data to the json document
        transaction.put("name", name);
        transaction.put("amount", amount);
        transaction.put("time", now.toString());
        return new ProducerRecord<>(TOPIC, name, transaction.toString());
    }
}
