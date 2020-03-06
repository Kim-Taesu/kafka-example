package common;

import config.Config;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class TopicUtils {

    private static Logger log = LoggerFactory.getLogger(TopicUtils.class);

    public static void createTopic(Properties properties, NewTopic newTopic) throws InterruptedException {
        AdminClient adminClient = AdminClient.create(properties);
        List<NewTopic> newTopics = new ArrayList<>();
        newTopics.add(newTopic);
        adminClient.createTopics(newTopics);
        adminClient.close();
        log.info("create topic " + newTopic.name() + " success");
        Thread.sleep(1000);
    }

    public static void deleteTopics(Properties properties, final String... topicNames) throws InterruptedException {
        AdminClient adminClient = KafkaAdminClient.create(properties);
        final DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(topicNames));
        deleteTopicsResult.values().keySet().forEach(System.out::println);
        log.info("Topics '{}' deleted.", Arrays.asList(topicNames));
        Thread.sleep(1000);
    }

    public static void describeTopic(Properties properties, final String topicNames) throws InterruptedException {
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
                    System.out.println("\tleader\t" + partitionInfo.leader());
                    System.out.println("\treplicas\t" + Utils.join(partitionInfo.replicas(), ", "));
                    System.out.println("\tisr\t" + Utils.join(partitionInfo.isr(), ", "));
                });
            });
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        Thread.sleep(1000);
    }

    public static void listTopic(Properties properties) throws InterruptedException {
        AdminClient adminClient = KafkaAdminClient.create(properties);
        final ListTopicsResult listTopicsResult = adminClient.listTopics();
        try {
            final Set<String> strings = listTopicsResult.names().get();
            strings.forEach(System.out::println);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        Thread.sleep(1000);
    }

    public static void updatePartition(Properties properties) throws InterruptedException {
        AdminClient adminClient = KafkaAdminClient.create(properties);
        Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments = new HashMap<>();
        TopicPartition partition_0 = new TopicPartition(Config.TEST_TOPIC, 0);
        TopicPartition partition_1 = new TopicPartition(Config.TEST_TOPIC, 1);
        TopicPartition partition_2 = new TopicPartition(Config.TEST_TOPIC, 2);
        reassignments.put(partition_0, null);
        reassignments.put(partition_1, null);
        reassignments.put(partition_2, null);
        final AlterPartitionReassignmentsResult alterPartitionReassignmentsResult = adminClient.alterPartitionReassignments(reassignments);
        alterPartitionReassignmentsResult.values().keySet().forEach(System.out::println);
        Thread.sleep(1000);
    }

    public static void main(String[] args) throws InterruptedException {
        NewTopic newTopic = new NewTopic(Config.TEST_TOPIC, 2, (short) 1);
        final String inputTopic = "streams-plaintext-input";
        final NewTopic input = new NewTopic(inputTopic, 2, (short) 1);
        final String outputTopic = "streams-plaintext-onput";
        final NewTopic output = new NewTopic(outputTopic, 2, (short) 1);

        Properties properties = new Properties();
        properties.put(Config.GROUP_ID, Config.TEST_CONSUMER_GROUP);
        properties.put(Config.BOOTSTRAP_SERVERS, Config.KAFKA_SERVERS);
        properties.put(Config.KEY_DESERIALIZER, Config.STRING_DESERIALIZER);
        properties.put(Config.VALUE_DESERIALIZER, Config.STRING_DESERIALIZER);
        properties.put(Config.ENABLE_AUTO_COMMIT, Config.TRUE);
        properties.put(Config.AUTO_OFFSET_RESET, Config.LATEST);

//        deleteTopics(properties, outputTopic);
//        createTopic(properties, newTopic);
        listTopic(properties);
//        updatePartition(properties);
//        describeTopic(properties, Config.TEST_TOPIC);
    }
}
