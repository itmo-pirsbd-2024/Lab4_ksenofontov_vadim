package ru.itmo.advancedjava.grpcdemo.service;

import static java.util.stream.Collectors.toUnmodifiableMap;

import io.grpc.BindableService;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import ru.itmo.advancedjava.grpcdemo.proto.*;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;



public final class UsersTrackerService extends UsersTrackerServiceGrpc.UsersTrackerServiceImplBase {

    ConcurrentLinkedQueue<ConsumerRecord<String, UsersTrackerService.MostFilledSectorsRecord>> mostFilledSectorsQueueLink;
    ConcurrentHashMap<Long, ConcurrentLinkedQueue<MotionPointRecord>> motionPointRecordQueuesLink;

    public UsersTrackerService(ConcurrentLinkedQueue<ConsumerRecord<String, UsersTrackerService.MostFilledSectorsRecord>> mostFilledSectorsQueue,
                               ConcurrentHashMap<Long, ConcurrentLinkedQueue<MotionPointRecord>> motionPointRecordQueues) {
        super();
        mostFilledSectorsQueueLink = mostFilledSectorsQueue;
        motionPointRecordQueuesLink = motionPointRecordQueues;
    }


    public static class MotionPointRecord implements Deserializer<MotionPointRecord> {
        public Integer x;
        public Integer y;
        public Integer z;

        public MotionPointRecord() {
            super();
        }

        @Override
        public MotionPointRecord deserialize(String topic, byte[] bytes) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes); // big-endian by default

            var res = new MotionPointRecord();
            res.x = buffer.getInt();
            res.y = buffer.getInt();
            res.z = buffer.getInt();

            return res;
        }
    }


    public static class MostFilledSectorsRecord implements Deserializer<MostFilledSectorsRecord> {
        public ArrayList<Integer> x;
        public ArrayList<Integer> y;
        public ArrayList<Long> number;

        public MostFilledSectorsRecord() {
            super();
            x = new ArrayList<>();
            y = new ArrayList<>();
            number = new ArrayList<>();
        }

        @Override
        public MostFilledSectorsRecord deserialize(String topic, byte[] bytes) {

            ByteBuffer value = ByteBuffer.wrap(bytes); // big-endian by default

            var res = new MostFilledSectorsRecord();
            System.out.println("BYTES_SIZE: " + bytes.length);
            for (long i = 0; i < bytes.length / (4 + 4 + 8); i++){
                res.x.add(value.getInt());
                res.y.add(value.getInt());
                res.number.add(value.getLong());
            }

            return res;
        }
    }



    public static BindableService create(ConcurrentLinkedQueue<ConsumerRecord<String, UsersTrackerService.MostFilledSectorsRecord>> mostFilledSectorsQueue,
                                         ConcurrentHashMap<Long, ConcurrentLinkedQueue<MotionPointRecord>> motionPointRecordQueues) {
        return new UsersTrackerService(mostFilledSectorsQueue, motionPointRecordQueues);
    }

    @Override
    public void getUserPositions(UserID request, StreamObserver<Position> responseObserver) {

        System.out.println("UserID:  " + request.getId());
        motionPointRecordQueuesLink.putIfAbsent(request.getId(), new ConcurrentLinkedQueue<>());

        System.out.println("Queues:  " + motionPointRecordQueuesLink.keySet());
        while (true) {
            var motionPointRecord = motionPointRecordQueuesLink.get(request.getId()).poll();

            if (motionPointRecord != null) {
                var res = Position.newBuilder().setX(motionPointRecord.x).setY(motionPointRecord.y).build();

                responseObserver.onNext(res);

                System.out.println("[ " + res.getX() + ", " + res.getY() + " ]");
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

    }


    @Override
    public void getMostFilledSectors(UserID request, StreamObserver<MostFilledSectors> responseObserver) {
        long lastProcessedTimestamp = 0;
        while (true) {
            var currentMostFilledSectors = mostFilledSectorsQueueLink.peek();

            if (currentMostFilledSectors != null) {
                if (currentMostFilledSectors.timestamp() > lastProcessedTimestamp) {
                    lastProcessedTimestamp = currentMostFilledSectors.timestamp();

                    var sectors = new ArrayList<Sector>();

                    for (int i = 0; i < currentMostFilledSectors.value().y.size(); i++) {
                        sectors.add(Sector.newBuilder()
                                .setX(currentMostFilledSectors.value().x.get(i))
                                .setY(currentMostFilledSectors.value().y.get(i))
                                .setMessagesGot(currentMostFilledSectors.value().number.get(i))
                                .build());
                    }

                    var res = MostFilledSectors.newBuilder().addAllSectors(sectors).build();

                    responseObserver.onNext(res);


                    System.out.println("<");
                    for (var sector : res.getSectorsList()) {
                        System.out.println("[ " + sector.getX() + ", " + sector.getY() + " ] : " + sector.getMessagesGot());
                    }
                    System.out.println(">");

                }
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
