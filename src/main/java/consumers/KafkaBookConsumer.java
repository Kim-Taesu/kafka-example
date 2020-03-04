package consumers;

import config.Config;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaBookConsumer {
    public static void main(String[] args) {
        final String topicName = "kts-topic";
        final String consumerGroup = "kts-consumer";

        Properties properties = new Properties();
        properties.put(Config.GROUP_ID, consumerGroup);
        properties.put(Config.ENABLE_AUTO_COMMIT, Config.TRUE);
        properties.put(Config.AUTO_OFFSET_RESET, Config.LATEST);
        properties.put(Config.BOOTSTRAP_SERVERS, Config.KAFKA_SERVERS);
        properties.put(Config.KEY_DESERIALIZER, Config.STRING_DESERIALIZER);
        properties.put(Config.VALUE_DESERIALIZER, Config.STRING_DESERIALIZER);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        try (consumer) {
            consumer.subscribe(Arrays.asList(topicName));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records)
                    System.out.printf("TimeStamp: %s\tTopic: %s, Partition: %s, Offset: %d, Key: %s, Value: %s\n", record.timestamp(), record.topic(), record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }
}
