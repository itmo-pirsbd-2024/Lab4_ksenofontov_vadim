package ru.itmo.advancedjava.grpcdemo;

import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.itmo.advancedjava.grpcdemo.proto.MostFilledSectors;
import ru.itmo.advancedjava.grpcdemo.proto.Position;
import ru.itmo.advancedjava.grpcdemo.proto.Sector;
import ru.itmo.advancedjava.grpcdemo.service.UsersTrackerService;


public class GrpcDemoServerMain {

    public static void main(String[] args) {
        new GrpcDemoServerMain().run();
    }

    public void run() {

        ConcurrentLinkedQueue<ConsumerRecord<String, UsersTrackerService.MostFilledSectorsRecord>> mostFilledSectorsQueue = new ConcurrentLinkedQueue<>();
        ConcurrentHashMap<Long, ConcurrentLinkedQueue<UsersTrackerService.MotionPointRecord>> motionPointRecordQueues = new ConcurrentHashMap<>();

        var service = UsersTrackerService.create(mostFilledSectorsQueue, motionPointRecordQueues);

        var server = ServerBuilder.forPort(8080)
                .addService(service)
                .addService(ProtoReflectionServiceV1.newInstance())
                .build();

        try {
            var one = new Thread() {
                public void run() {

                    String bootstrapServer = "127.0.0.1:9092";
                    String groupId = "consumer-group";
                    String topics = "most_filled_sectors";

                    Properties properties = new Properties();
                    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
                    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UsersTrackerService.MostFilledSectorsRecord.class.getName());
                    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
                    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

                    KafkaConsumer<String, UsersTrackerService.MostFilledSectorsRecord> consumer = new KafkaConsumer<>(properties);

                    consumer.subscribe(List.of(topics));


                    // Poll the data
                    while (true) {
                        ConsumerRecords<String, UsersTrackerService.MostFilledSectorsRecord> records = consumer.poll(Duration.ofMillis(100));

                        for (ConsumerRecord<String, UsersTrackerService.MostFilledSectorsRecord> record : records) {

                            if (mostFilledSectorsQueue.size() == 20) {
                                mostFilledSectorsQueue.poll();
                            }

                            mostFilledSectorsQueue.add(record);

                            System.out.println("Key: " + record.key() +
                                    " Value: " + record.value() +
                                    " Partition: " + record.partition() +
                                    " Offset: " + record.offset());
                        }
                    }

                }
            };
            one.start();


            var two = new Thread() {
                public void run() {

                    String bootstrapServer = "127.0.0.1:9092";
                    String groupId = "gfg-consumer-group";
                    String topics = "motion_point";

                    Properties properties = new Properties();
                    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
                    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
                    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UsersTrackerService.MotionPointRecord.class.getName());
                    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
                    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

                    KafkaConsumer<Long, UsersTrackerService.MotionPointRecord> consumer = new KafkaConsumer<>(properties);

                    consumer.subscribe(List.of(topics));

                    // Poll the data
                    while (true) {
                        ConsumerRecords<Long, UsersTrackerService.MotionPointRecord> records = consumer.poll(Duration.ofMillis(100));

                        for (ConsumerRecord<Long, UsersTrackerService.MotionPointRecord> record : records) {

                            System.out.println("Key: " + record.key() +
                                    " Value: [ " + record.value().x + ", " + record.value().y + " ]" +
                                    " Partition: " + record.partition() +
                                    " Offset: " + record.offset());

                            if (motionPointRecordQueues.containsKey(record.key())) {

                                if (motionPointRecordQueues.get(record.key()).size() == 50) {
                                    motionPointRecordQueues.get(record.key()).poll();
                                }

                                motionPointRecordQueues.get(record.key()).add(record.value());

                                System.out.println("Key: " + record.key() +
                                        " Value: " + record.value() +
                                        " Partition: " + record.partition() +
                                        " Offset: " + record.offset());
                            }
                        }
                    }

                }
            };
            two.start();


            server.start().awaitTermination();

            //responseObserver.onCompleted();

        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted", e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }



        // --------------------------------------------------------------------------------



        //responseObserver.onCompleted();



    }
}