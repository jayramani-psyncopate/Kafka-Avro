package com.kafkaavroschema.consumer;

import com.kafkaavroschema.dto.Employee;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;
import org.springframework.messaging.handler.annotation.Header;


@Service
@Slf4j
public class KafkaAvroConsumer {

    @RetryableTopic(attempts = "5", exclude = {NullPointerException.class}, backoff = @Backoff(delay = 2000, multiplier = 2.0))
    @KafkaListener(topics = "kafka-avro")
    public void read(ConsumerRecord<String, GenericRecord> consumerRecord, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.OFFSET) long offset) {

        GenericRecord v = consumerRecord.value();
        Long id = (Long) v.get("id");
        CharSequence name = (CharSequence) v.get("name");
        CharSequence dept = (CharSequence) v.get("department");
        Long updatedAt = (Long) v.get("updated_at");

        if (name.toString().isEmpty()) {
            throw new IllegalStateException("Forcing retries (email ends with @retry.me)");
        }

        log.info("Received record: {}, {}", name, dept);

    }

    @DltHandler
    public void listenDLT(ConsumerRecord<String, Employee> consumerRecord, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.OFFSET) long offset) {
        log.info("DLT Received : {} , from {} , offset {}", consumerRecord.value().getId(),topic,offset);
    }

    @KafkaListener(topics = "mysql-employees")
    public void readEmployee(ConsumerRecord<String, GenericRecord> rec) {
        GenericRecord v = rec.value();
        Long id = (Long) v.get("id");
        CharSequence name = (CharSequence) v.get("name");
        CharSequence dept = (CharSequence) v.get("department");
        Long updatedAt = (Long) v.get("updated_at");
        log.info("Received: key={}, id={}, name={}, dept={}, updated_at={}",
                rec.key(), id, name, dept, updatedAt);
    }
}
