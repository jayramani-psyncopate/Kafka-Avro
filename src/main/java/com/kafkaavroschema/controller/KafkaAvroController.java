package com.kafkaavroschema.controller;

import com.kafkaavroschema.dto.Employee;
import com.kafkaavroschema.producer.KafkaAvroProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaAvroController {

    @Autowired
    private KafkaAvroProducer kafkaProducer;

    @PostMapping("/send")
    public String sendMessage(@RequestBody Employee employee) {
        kafkaProducer.send(employee);
        return "Message sent to Kafka";
    }
}
