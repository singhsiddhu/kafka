package kafka.publisher;

import java.util.Properties;

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import kafka.config.IKafkaConstants;

public class KafkaPublisher {
    private static Properties getPubConfig() {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAKFA_BROKERS);
        prop.put(ProducerConfig.CLIENT_ID_CONFIG, IKafkaConstants.CLIENT_ID);
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());// Object.class.getName());//
                                                                                             // LongSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put("acks", "all");
        return prop;
    }

    public static KafkaProducer<Long, String> getPublisher() {
        KafkaProducer<Long, String> kafkaProducer = new KafkaProducer<Long, String>(KafkaPublisher.getPubConfig());
        return kafkaProducer;
    }

    public static void closeProducer(KafkaProducer<Long, String> producer) {
        if (producer != null) {
            producer.close();
        }
    }
}
