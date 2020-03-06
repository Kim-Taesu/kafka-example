package consumers;

import config.Config;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaBookConsumer {


    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(Config.GROUP_ID, Config.TEST_CONSUMER_GROUP);
        properties.put(Config.ENABLE_AUTO_COMMIT, Config.FALSE);
        properties.put(Config.AUTO_OFFSET_RESET, Config.EARLIEST);
        properties.put(Config.BOOTSTRAP_SERVERS, Config.KAFKA_SERVERS);
        properties.put(Config.KEY_DESERIALIZER, Config.STRING_DESERIALIZER);
        properties.put(Config.VALUE_DESERIALIZER, Config.STRING_DESERIALIZER);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        Properties anotherProperties = new Properties();
        anotherProperties.put(Config.GROUP_ID, Config.TEST_CONSUMER_GROUP + "2");
        anotherProperties.put(Config.ENABLE_AUTO_COMMIT, Config.FALSE);
        anotherProperties.put(Config.AUTO_OFFSET_RESET, Config.EARLIEST);
        anotherProperties.put(Config.BOOTSTRAP_SERVERS, Config.KAFKA_SERVERS);
        anotherProperties.put(Config.KEY_DESERIALIZER, Config.STRING_DESERIALIZER);
        anotherProperties.put(Config.VALUE_DESERIALIZER, Config.STRING_DESERIALIZER);
        KafkaConsumer<String, String> anotherConsumer = new KafkaConsumer<>(anotherProperties);

        // Topic Partition 설정
        TopicPartition partition_0 = new TopicPartition(Config.TEST_TOPIC, 0);
        TopicPartition partition_1 = new TopicPartition(Config.TEST_TOPIC, 1);
        consumer.assign(Collections.singleton(partition_0));
        anotherConsumer.assign(Collections.singleton(partition_1));

        // Topic Partition offset 설정
        consumer.seek(partition_0, 0);
        anotherConsumer.seek(partition_1, 0);

        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yy-MM-dd hh:mm:ss.SSS");


        try (consumer) {
//            consumer.subscribe(Arrays.asList(topicName));
//            anotherConsumer.subscribe(Arrays.asList(topicName));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                ConsumerRecords<String, String> records2 = anotherConsumer.poll(Duration.ofMillis(50));
                for (ConsumerRecord<String, String> record : records)
                    System.out.printf("ConsumerGroup: %s\tTimeStamp: %s\tTopic: %s\tPartition: %s\tOffset: %d\tKey: %s\tValue: %s\n", properties.get(Config.GROUP_ID), simpleDateFormat.format(record.timestamp()), record.topic(), record.partition(), record.offset(), record.key(), record.value());
                for (ConsumerRecord<String, String> record : records2)
                    System.out.printf("ConsumerGroup: %s\tTimeStamp: %s\tTopic: %s\tPartition: %s\tOffset: %d\tKey: %s\tValue: %s\n", anotherProperties.get(Config.GROUP_ID), simpleDateFormat.format(record.timestamp()), record.topic(), record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }
}
