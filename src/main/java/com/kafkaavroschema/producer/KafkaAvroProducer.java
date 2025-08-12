package com.kafkaavroschema.producer;

import com.kafkaavroschema.dto.Employee;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class KafkaAvroProducer {

    @Autowired
    private KafkaTemplate<String, Employee> kafkaTemplate;

    public void send(Employee employee) {

        CompletableFuture<SendResult<String, Employee>> send = kafkaTemplate.send("kafka-avro", UUID.randomUUID().toString(),employee);
        send.whenComplete((r, e) -> {
            if (e == null) {
                log.info(String.format("Sending employee to Kafka: %s", employee.getId()));
            }else    {
                log.info(String.format("Sending employee to Kafka: %s has failed", employee.getId()));
            }
        });
    }
}