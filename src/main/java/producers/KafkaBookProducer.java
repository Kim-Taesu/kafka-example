package producers;

import config.Config;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Arrays;
import java.util.List;
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

    public static void produceSync2(Properties properties, String topicName) {
        final List<String> strings = Arrays.asList("a", "b", "c", "d", "e", "f", "g");
        strings.forEach(c -> {
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
                final RecordMetadata metadata = producer.send(new ProducerRecord<>(topicName, c)).get();
                System.out.println("Partition : " + metadata.partition() + ", Offset :" + metadata.offset() + "");
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });
    }

    public static void produceSyncByKey(Properties properties, String topicName) {
        for (int i = 1; i < 11; i++) {
            String key = String.valueOf(i % 3);
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
                final RecordMetadata metadata = producer.send(
                        new ProducerRecord<>(topicName, key, String.format("%d", i))
                ).get();
                System.out.println("Partition : " + metadata.partition() + ", Offset :" + metadata.offset() + "");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void produceAsync(Properties properties, String topicName) {
        try (final KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int i = 0; i < 11; i++) {
                producer.send(
                        new ProducerRecord<>(topicName, "produce test " + i),
                        new AsyncCallback());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(Config.ACKS, "1");
        properties.put(Config.BOOTSTRAP_SERVERS, Config.KAFKA_SERVERS);
        properties.put(Config.KEY_SERIALIZER, Config.STRING_SERIALIZER);
        properties.put(Config.VALUE_SERIALIZER, Config.STRING_SERIALIZER);
//        produceSync(properties, Config.TEST_TOPIC);
//        produceSync2(properties, Config.TEST_TOPIC);
        produceSyncByKey(properties, Config.TEST_TOPIC);
//        produceAsync(properties, Config.TEST_TOPIC);
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
}
