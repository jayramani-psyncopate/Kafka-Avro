package com.kafkaavroschema.consumer;

import com.kafkaavroschema.dto.Employee;
import lombok.extern.slf4j.Slf4j;
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
    public void read(ConsumerRecord<String, Employee> consumerRecord, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.OFFSET) long offset) {

        String key = consumerRecord.key();
        Employee employee = consumerRecord.value();

        if (employee.getEmailId().toString().endsWith("@retry.me")) {
            throw new IllegalStateException("Forcing retries (email ends with @retry.me)");
        }

        log.info("Received record: {}, {}", key, employee);

    }

    @DltHandler
    public void listenDLT(ConsumerRecord<String, Employee> consumerRecord, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.OFFSET) long offset) {
        log.info("DLT Received : {} , from {} , offset {}", consumerRecord.value().getEmailId(),topic,offset);
    }
}
