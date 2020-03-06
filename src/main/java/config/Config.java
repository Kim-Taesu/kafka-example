package config;

public class Config {
    public static final String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    public static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String KAFKA_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String KEY_DESERIALIZER = "key.deserializer";
    public static final String KEY_SERIALIZER = "key.serializer";
    public static final String VALUE_DESERIALIZER = "value.deserializer";
    public static final String VALUE_SERIALIZER = "value.serializer";
    public static final String GROUP_ID = "group.id";
    public static final String ENABLE_AUTO_COMMIT = "enable.auto.commit";
    public static final String ACKS = "acks";

    public static final String AUTO_OFFSET_RESET = "auto.offset.reset";

    public static final String TRUE = "true";
    public static final String FALSE = "false";
    public static final String LATEST = "latest";
    public static final String EARLIEST = "earliest";

    public static final String TEST_TOPIC = "test-topic";
    public static final String TEST_CONSUMER_GROUP = "consumer group";
}
