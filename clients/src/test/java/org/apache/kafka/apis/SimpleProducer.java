package org.apache.kafka.apis;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SimpleProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //props.put("partitioner.class", "org.apache.kafka.apis.SinglePartitionPartitioner");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        while (true) {
            try {
                producer.send(new ProducerRecord<String, String>("test", System.currentTimeMillis() + "", new Date().toString()));
                                producer.send(new ProducerRecord<String, String>("test2", System.currentTimeMillis() + "", new Date().toString()));
                TimeUnit.MILLISECONDS.sleep(100);
                //System.out.println("send records finish ... ");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
