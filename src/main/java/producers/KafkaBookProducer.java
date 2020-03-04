package producers;

import config.Config;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaBookProducer {
    public static void produceSync(Properties properties, String topicName) {
        String sample = "Apache Kafka is a distributed streaming platform";
        for (int i = 0; i < 11; i++) {
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
                final RecordMetadata metadata = producer.send(new ProducerRecord<>(topicName, sample)).get();
                System.out.println("Partition : " + metadata.partition() + ", Offset :" + metadata.offset() + "");
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

        }
    }

    public static void produceSyncByKey(Properties properties, String topicName) {
        for (int i = 1; i < 11; i++) {
            String key = String.valueOf(i % 2);
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
                final RecordMetadata metadata = producer.send(
                        new ProducerRecord<>(topicName, key, String.format("%d - Apache Kafka is a distributed streaming platform - key=" + key, i))
                ).get();
                System.out.println("Partition : " + metadata.partition() + ", Offset :" + metadata.offset() + "");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static class AsyncCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (metadata != null) {
                System.out.println("Partition : " + metadata.partition() + ", Offset :" + metadata.offset() + "");
            } else {
                exception.printStackTrace();
            }
        }
    }

    public static void produceAsync(Properties properties, String topicName) {
        try (final KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int i = 0; i < 11; i++) {
                producer.send(
                        new ProducerRecord<>(topicName, "produce test "+i),
                        new AsyncCallback());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        final String topicName = "kts-topic";

        Properties properties = new Properties();
        properties.put(Config.ACKS, "1");
        properties.put(Config.BOOTSTRAP_SERVERS, Config.KAFKA_SERVERS);
        properties.put(Config.KEY_SERIALIZER, Config.STRING_SERIALIZER);
        properties.put(Config.VALUE_SERIALIZER, Config.STRING_SERIALIZER);
//        produceSync(properties, topicName);
//        produceSyncByKey(properties, topicName);
        produceAsync(properties, topicName);
    }
}
