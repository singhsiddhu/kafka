package kafka;

import static kafka.publisher.KafkaPublisher.closeProducer;
import static kafka.publisher.KafkaPublisher.getPublisher;
import static kafka.subscriber.KafkaSubscriber.closeConsumer;
import static kafka.subscriber.KafkaSubscriber.getSubscriber;

import java.time.Duration;
import java.util.Collections;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import kafka.config.IKafkaConstants;

/**
 * Hello world!
 *
 */
public class App {

    private static KafkaProducer<Long, String> aProducer;
    private static KafkaConsumer<Long, String> aConsumer;

    public static void main(String[] args) {
        System.out.println("Hello World!");
        aProducer = getPublisher();
        aConsumer = getSubscriber();

        // Spin Off consumer first then run Prodcuer
        Runnable runnable = () -> {
            System.out.println("Starting Consumer...");
            runConsumer();
        };
        Thread consumerThread = new Thread(runnable);
        consumerThread.start();
        runProducer();
    }

    static void runConsumer() {
        // aConsumer.subscribe(Arrays.asList(IKafkaConstants.TOPIC_NAME));
        aConsumer.subscribe(Collections.singletonList(IKafkaConstants.TOPIC_NAME));
        try {
            while (true) {
                // System.out.println("Consumer reading record...");
                ConsumerRecords<Long, String> records = aConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<Long, String> record : records) {
                    System.out.println("Consumer : Record with Key : " + record.key() + " and Value : " + record.value());
                }
                aConsumer.commitSync();
            }
        } finally {
            closeConsumer(aConsumer);
        }
    }

    static void runProducer() {
        Scanner userInput = new Scanner(System.in);
        String line;

        System.out.print("Enter Key(Long)-Value(String) : ");
        while ("" != (line = userInput.nextLine().trim())) {
            String[] keyValue = line.split("-", -2);
            ProducerRecord<Long, String> record = null;
            try {
                record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME, Long.parseLong(keyValue[0]),
                        keyValue[1]);
                aProducer.send(record);
                System.out.println("Published record :- Key : " + record.key() + " Value : " + record.value());
                System.out.print("Enter Key(Long)-Value(String) : ");
            } catch (Exception e) {
                System.err.println("Exception : " + e.getMessage());
            }
        }
        userInput.close();
        closeProducer(aProducer);
    }
}
