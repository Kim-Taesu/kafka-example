package common;

import config.Config;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class TopicUtils {

    private static Logger log = LoggerFactory.getLogger(TopicUtils.class);

    public static void createTopic(Properties properties, NewTopic newTopic) {
        AdminClient adminClient = AdminClient.create(properties);
        List<NewTopic> newTopics = new ArrayList<>();
        newTopics.add(newTopic);
        adminClient.createTopics(newTopics);
        adminClient.close();
        log.info("create topic " + newTopic.name() + " success");
    }

    public static void deleteTopics(Properties properties, final String... topicNames) {
        AdminClient adminClient = KafkaAdminClient.create(properties);
        final DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(topicNames));
        log.info("Topics '{}' deleted.", Arrays.asList(topicNames));
    }

    public static void describeTopic(Properties properties, final String topicNames) {
        AdminClient adminClient = KafkaAdminClient.create(properties);
        final DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singletonList(topicNames));
        try {
            final Map<String, TopicDescription> info = describeTopicsResult.all().get();
            info.keySet().forEach(e -> {
                final TopicDescription description = info.get(e);
                System.out.println("name\t" + description.name());
                System.out.println("internal\t" + description.isInternal());
                System.out.println("authorizedOperations\t" + description.authorizedOperations());
                final List<TopicPartitionInfo> partitions = description.partitions();
                partitions.forEach(partitionInfo -> {
                    System.out.println("partition\t" + partitionInfo.partition());
                    System.out.println("leader\t" + partitionInfo.leader());
                    System.out.println("replicas\t" + Utils.join(partitionInfo.replicas(), ", "));
                    System.out.println("isr\t" + Utils.join(partitionInfo.isr(), ", "));
                });
            });
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        final String consumerGroup = "test-consumer-group";
        final String topicName = "test-topic";
        NewTopic newTopic = new NewTopic(topicName, 2, (short) 2);

        Properties properties = new Properties();
        properties.put(Config.GROUP_ID, consumerGroup);
        properties.put(Config.BOOTSTRAP_SERVERS, Config.KAFKA_SERVERS);
        properties.put(Config.KEY_DESERIALIZER, Config.STRING_DESERIALIZER);
        properties.put(Config.VALUE_DESERIALIZER, Config.STRING_DESERIALIZER);
        properties.put(Config.ENABLE_AUTO_COMMIT, Config.TRUE);
        properties.put(Config.AUTO_OFFSET_RESET, Config.LATEST);

        deleteTopics(properties, topicName);
        Thread.sleep(500);
        createTopic(properties, newTopic);
        Thread.sleep(500);
        describeTopic(properties, topicName);
    }
}
