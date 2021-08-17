package kafka.subscriber;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import kafka.config.IKafkaConstants;

public class KafkaSubscriber {

    private static Properties getSubConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAKFA_BROKERS);
        props.put("group.id", IKafkaConstants.GROUP_ID_CONFIG);
        props.put("enable.auto.commit", "false");
        // props.put("auto.commit.interval.ms", "1000");
        // props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", LongDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        return props;
    }

    public static KafkaConsumer<Long, String> getSubscriber() {
        KafkaConsumer<Long, String> kafkaConsumer = new KafkaConsumer<Long, String> (KafkaSubscriber.getSubConfig());
        return kafkaConsumer;
    }

    public static void closeConsumer(KafkaConsumer<Long, String> consumer) {
        if (consumer != null) {
            consumer.close();
        }
    }
}
