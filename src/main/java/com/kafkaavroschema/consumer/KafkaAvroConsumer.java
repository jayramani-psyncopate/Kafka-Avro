package com.kafkaavroschema.consumer;

import com.kafkaavroschema.dto.Employee;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaAvroConsumer {


    @KafkaListener(topics = "kafka-avro")
    public void read(ConsumerRecord<String, Employee> consumerRecord) {

        String key = consumerRecord.key();
        Employee employee = consumerRecord.value();

        log.info("Received record: " + key + ", " + employee);

    }
}
